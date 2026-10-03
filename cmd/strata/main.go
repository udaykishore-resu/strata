// Command strata is the CLI for the Strata deployment engine.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"text/tabwriter"
	"time"

	"github.com/udaykishore-resu/strata/pkg/client"
	"github.com/udaykishore-resu/strata/pkg/template"
)

var version = "dev"

const usage = `strata — deploy GCP infrastructure stacks with the Strata engine

Usage:
  strata <command> [flags]

Commands:
  deploy     Plan, confirm and apply a template to a stack
  diff       Show the change set a template would produce
  destroy    Delete a stack and its resources
  describe   Show a stack's status, resources and outputs
  list       List stacks
  events     Show a stack's event history
  drift      Compare live resources with the applied state
  validate   Validate a template locally
  types      List supported resource types
  version    Print the CLI version

Global flags (or environment):
  --server  Strata API URL            (STRATA_SERVER, default http://localhost:8080)
  --token   Bearer token              (STRATA_TOKEN; otherwise ` + "`gcloud auth print-identity-token`" + ` for remote servers)

Run 'strata <command> -h' for command flags.
`

type params map[string]any

func (p params) String() string { return "" }
func (p params) Set(v string) error {
	k, val, ok := strings.Cut(v, "=")
	if !ok || k == "" {
		return fmt.Errorf("parameter must be KEY=VALUE, got %q", v)
	}
	var parsed any
	if err := json.Unmarshal([]byte(val), &parsed); err == nil {
		switch parsed.(type) {
		case float64, bool:
			p[k] = parsed
			return nil
		}
	}
	p[k] = val
	return nil
}

type cli struct {
	server string
	token  string
	out    io.Writer
	color  bool
}

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	c := &cli{out: os.Stdout, color: isTerminal(os.Stdout) && os.Getenv("NO_COLOR") == ""}
	global := flag.NewFlagSet("strata", flag.ContinueOnError)
	global.Usage = func() { fmt.Fprint(os.Stderr, usage) }
	global.StringVar(&c.server, "server", envOr("STRATA_SERVER", "http://localhost:8080"), "API URL")
	global.StringVar(&c.token, "token", os.Getenv("STRATA_TOKEN"), "bearer token")
	if err := global.Parse(args); err != nil {
		return 2
	}
	rest := global.Args()
	if len(rest) == 0 {
		fmt.Fprint(os.Stderr, usage)
		return 2
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	cmds := map[string]func(context.Context, []string) error{
		"deploy": c.deploy, "diff": c.diff, "destroy": c.destroy, "describe": c.describe,
		"list": c.list, "events": c.events, "drift": c.drift, "validate": c.validate,
		"types": c.types, "version": func(context.Context, []string) error { fmt.Fprintln(c.out, "strata", version); return nil },
	}
	fn, ok := cmds[rest[0]]
	if !ok {
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n%s", rest[0], usage)
		return 2
	}
	err := fn(ctx, rest[1:])
	var ec exitCode
	switch {
	case err == nil:
		return 0
	case errors.As(err, &ec):
		return int(ec)
	case errors.Is(err, flag.ErrHelp):
		return 0
	default:
		fmt.Fprintln(os.Stderr, c.paint("31", "error: ")+err.Error())
		return 1
	}
}

type exitCode int

func (e exitCode) Error() string { return fmt.Sprintf("exit %d", int(e)) }

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func isTerminal(f *os.File) bool {
	st, err := f.Stat()
	return err == nil && st.Mode()&os.ModeCharDevice != 0
}

func (c *cli) paint(code, s string) string {
	if !c.color {
		return s
	}
	return "\x1b[" + code + "m" + s + "\x1b[0m"
}

func (c *cli) client() *client.Client {
	cl := client.New(c.server)
	if c.token != "" {
		tok := c.token
		cl.Token = func(context.Context) (string, error) { return tok, nil }
		return cl
	}
	if u, err := url.Parse(c.server); err == nil {
		h := u.Hostname()
		if h == "localhost" || h == "127.0.0.1" || h == "::1" {
			return cl
		}
	}
	var once sync.Once
	var tok string
	var terr error
	cl.Token = func(ctx context.Context) (string, error) {
		once.Do(func() {
			cmdline := envOr("STRATA_TOKEN_COMMAND", "gcloud auth print-identity-token")
			parts := strings.Fields(cmdline)
			out, err := exec.CommandContext(ctx, parts[0], parts[1:]...).Output()
			if err != nil {
				terr = fmt.Errorf("%s: %w (set STRATA_TOKEN or STRATA_TOKEN_COMMAND)", cmdline, err)
				return
			}
			tok = strings.TrimSpace(string(out))
		})
		return tok, terr
	}
	return cl
}

func readTemplate(path string) (json.RawMessage, *template.Template, error) {
	if path == "" {
		return nil, nil, errors.New("-f template file is required (use - for stdin)")
	}
	var b []byte
	var err error
	if path == "-" {
		b, err = io.ReadAll(os.Stdin)
	} else {
		b, err = os.ReadFile(path)
	}
	if err != nil {
		return nil, nil, err
	}
	t, err := template.Parse(b)
	if err != nil {
		return nil, nil, err
	}
	return b, t, nil
}

type planFlags struct {
	fs      *flag.FlagSet
	stack   *string
	file    *string
	params  params
	verbose *bool
}

func newPlanFlags(name string) *planFlags {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	pf := &planFlags{fs: fs, params: params{}}
	pf.stack = fs.String("s", "", "stack name (required)")
	pf.file = fs.String("f", "", "template file, or - for stdin (required)")
	pf.verbose = fs.Bool("v", false, "show all property values, including creates")
	fs.Var(pf.params, "p", "parameter KEY=VALUE (repeatable)")
	return pf
}

func (pf *planFlags) parse(args []string) error {
	if err := pf.fs.Parse(args); err != nil {
		return err
	}
	if *pf.stack == "" {
		return errors.New("-s stack is required")
	}
	return nil
}

func (c *cli) plan(ctx context.Context, pf *planFlags) (*client.ChangeSet, error) {
	raw, _, err := readTemplate(*pf.file)
	if err != nil {
		return nil, err
	}
	cs, err := c.client().CreateChangeSet(ctx, *pf.stack, raw, pf.params)
	if err != nil {
		return nil, err
	}
	c.printPlan(cs, *pf.verbose)
	return cs, nil
}

func (c *cli) diff(ctx context.Context, args []string) error {
	pf := newPlanFlags("diff")
	detailed := pf.fs.Bool("detailed-exitcode", false, "exit 2 when there are changes (for CI)")
	if err := pf.parse(args); err != nil {
		return err
	}
	cs, err := c.plan(ctx, pf)
	if err != nil {
		return err
	}
	if *detailed && cs.HasChanges() {
		return exitCode(2)
	}
	return nil
}

func (c *cli) deploy(ctx context.Context, args []string) error {
	pf := newPlanFlags("deploy")
	yes := pf.fs.Bool("yes", false, "apply without confirmation")
	noWait := pf.fs.Bool("no-wait", false, "return after starting the operation")
	if err := pf.parse(args); err != nil {
		return err
	}
	cs, err := c.plan(ctx, pf)
	if err != nil {
		return err
	}
	if !cs.HasChanges() {
		fmt.Fprintln(c.out, "\nNo resource changes. Applying to record parameters and outputs.")
	}
	if !*yes && !confirm(fmt.Sprintf("\nDeploy change set %s to stack %q?", cs.ID, cs.Stack)) {
		return errors.New("deploy cancelled")
	}
	cl := c.client()
	last := c.lastEventID(ctx, cl, cs.Stack)
	op, err := cl.ExecuteChangeSet(ctx, cs.Stack, cs.ID)
	if err != nil {
		return err
	}
	fmt.Fprintf(c.out, "\nOperation %s started.\n", op.ID)
	if *noWait {
		return nil
	}
	return c.follow(ctx, cl, op, last)
}

func (c *cli) destroy(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("destroy", flag.ContinueOnError)
	stack := fs.String("s", "", "stack name (required)")
	yes := fs.Bool("yes", false, "delete without confirmation")
	noWait := fs.Bool("no-wait", false, "return after starting the operation")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *stack == "" {
		return errors.New("-s stack is required")
	}
	cl := c.client()
	st, err := cl.GetStack(ctx, *stack)
	if err != nil {
		return err
	}
	fmt.Fprintf(c.out, "Stack %q has %d resource(s); resources with deletionPolicy Retain are kept.\n", st.Name, len(st.Resources))
	if !*yes && !confirm(fmt.Sprintf("Delete stack %q and its resources?", st.Name)) {
		return errors.New("destroy cancelled")
	}
	last := c.lastEventID(ctx, cl, *stack)
	op, err := cl.DeleteStack(ctx, *stack)
	if err != nil {
		return err
	}
	fmt.Fprintf(c.out, "Operation %s started.\n", op.ID)
	if *noWait {
		return nil
	}
	return c.follow(ctx, cl, op, last)
}

func (c *cli) lastEventID(ctx context.Context, cl *client.Client, stack string) int64 {
	var last int64
	for {
		evs, err := cl.Events(ctx, stack, last, 1000)
		if err != nil || len(evs) == 0 {
			return last
		}
		last = evs[len(evs)-1].ID
		if len(evs) < 1000 {
			return last
		}
	}
}

func (c *cli) follow(ctx context.Context, cl *client.Client, op *client.Operation, after int64) error {
	final, err := cl.Wait(ctx, op, after, func(e client.Event) { c.printEvent(e) })
	if err != nil {
		return err
	}
	if final.Status == "SUCCEEDED" {
		fmt.Fprintln(c.out, c.paint("32", "\n✔ "+final.Kind+" succeeded: "+final.Result))
		if final.Kind == "DEPLOY" {
			if st, err := cl.GetStack(ctx, final.Stack); err == nil {
				c.printOutputs(st)
				if st.StatusReason != "" {
					fmt.Fprintln(c.out, c.paint("33", "warning: "+st.StatusReason))
				}
			}
		}
		return nil
	}
	fmt.Fprintln(c.out, c.paint("31", "\n✘ "+final.Kind+" failed: "+final.Result))
	if final.Error != "" {
		fmt.Fprintln(c.out, final.Error)
	}
	return exitCode(3)
}

func confirm(prompt string) bool {
	fmt.Print(prompt + " [y/N] ")
	line, _ := bufio.NewReader(os.Stdin).ReadString('\n')
	switch strings.ToLower(strings.TrimSpace(line)) {
	case "y", "yes":
		return true
	}
	return false
}

func (c *cli) printPlan(cs *client.ChangeSet, verbose bool) {
	fmt.Fprintf(c.out, "Change set %s for stack %q\n\n", cs.ID, cs.Stack)
	counts := map[string]int{}
	lidW, typW := 0, 0
	for _, ch := range cs.Changes {
		if ch.Action != "NoOp" || verbose {
			lidW, typW = max(lidW, len(ch.LogicalID)), max(typW, len(ch.Type))
		}
	}
	for _, ch := range cs.Changes {
		counts[ch.Action]++
		if ch.Action == "NoOp" && !verbose {
			continue
		}
		sym, color := " ", "0"
		switch ch.Action {
		case "Create":
			sym, color = "+", "32"
		case "Update":
			sym, color = "~", "33"
		case "Replace":
			sym, color = "±", "35"
		case "Delete":
			sym, color = "-", "31"
		}
		label := fmt.Sprintf("%s %-7s", sym, ch.Action)
		fmt.Fprintf(c.out, "  %s  %-*s  %-*s  %s\n", c.paint(color, label), lidW, ch.LogicalID, typW, ch.Type, ch.PhysicalID)
		if ch.Action == "Create" && !verbose {
			continue
		}
		for _, d := range ch.Diffs {
			newV := fmtValue(d.New)
			if d.KnownLater {
				newV = "(known after apply)"
			}
			if ch.Action == "Create" {
				fmt.Fprintf(c.out, "               %s = %s\n", d.Name, newV)
				continue
			}
			note := ""
			if d.ForcesNew {
				note = c.paint("35", "  (forces replacement)")
			}
			fmt.Fprintf(c.out, "               %s: %s → %s%s\n", d.Name, fmtValue(d.Old), newV, note)
		}
	}
	fmt.Fprintf(c.out, "\nPlan: %d to create, %d to update, %d to replace, %d to delete, %d unchanged.\n",
		counts["Create"], counts["Update"], counts["Replace"], counts["Delete"], counts["NoOp"])
}

func fmtValue(v any) string {
	if v == nil {
		return "(none)"
	}
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprint(v)
	}
	s := string(b)
	if len(s) > 80 {
		s = s[:77] + "..."
	}
	return s
}

func (c *cli) printEvent(e client.Event) {
	color := "0"
	switch {
	case strings.HasSuffix(e.Status, "FAILED"):
		color = "31"
	case strings.HasSuffix(e.Status, "COMPLETE"):
		color = "32"
	case strings.HasSuffix(e.Status, "IN_PROGRESS"):
		color = "36"
	}
	target := e.Stack
	if e.LogicalID != "" {
		target = e.LogicalID
		if e.PhysicalID != "" {
			target += " (" + e.PhysicalID + ")"
		}
	}
	line := fmt.Sprintf("%s  %-22s %s", e.Timestamp.Local().Format("15:04:05"), c.paint(color, e.Status), target)
	if e.Reason != "" {
		line += "  " + c.paint("2", e.Reason)
	}
	fmt.Fprintln(c.out, line)
}

func (c *cli) printOutputs(st *client.Stack) {
	if len(st.Outputs) == 0 {
		return
	}
	fmt.Fprintln(c.out, "\nOutputs:")
	keys := make([]string, 0, len(st.Outputs))
	for k := range st.Outputs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(c.out, "  %s = %v\n", k, st.Outputs[k])
	}
}

func (c *cli) describe(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("describe", flag.ContinueOnError)
	stack := fs.String("s", "", "stack name (required)")
	asJSON := fs.Bool("json", false, "print raw JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *stack == "" {
		return errors.New("-s stack is required")
	}
	st, err := c.client().GetStack(ctx, *stack)
	if err != nil {
		return err
	}
	if *asJSON {
		enc := json.NewEncoder(c.out)
		enc.SetIndent("", "  ")
		return enc.Encode(st)
	}
	fmt.Fprintf(c.out, "Stack:    %s\nStatus:   %s\nVersion:  %d\nUpdated:  %s\n", st.Name, st.Status, st.Version, st.UpdatedAt.Local().Format(time.RFC3339))
	if st.StatusReason != "" {
		fmt.Fprintf(c.out, "Reason:   %s\n", st.StatusReason)
	}
	if st.CurrentOperation != "" {
		fmt.Fprintf(c.out, "Running:  %s\n", st.CurrentOperation)
	}
	fmt.Fprintln(c.out, "\nResources:")
	tw := tabwriter.NewWriter(c.out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "  LOGICAL ID\tTYPE\tPHYSICAL ID\tSTATUS")
	ids := make([]string, 0, len(st.Resources))
	for id := range st.Resources {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		r := st.Resources[id]
		fmt.Fprintf(tw, "  %s\t%s\t%s\t%s\n", id, r.Type, r.PhysicalID, r.Status)
	}
	tw.Flush()
	if len(st.PendingCleanup) > 0 {
		fmt.Fprintf(c.out, "\nPending cleanup: %d resource(s)\n", len(st.PendingCleanup))
	}
	c.printOutputs(st)
	return nil
}

func (c *cli) list(ctx context.Context, args []string) error {
	stacks, err := c.client().ListStacks(ctx)
	if err != nil {
		return err
	}
	tw := tabwriter.NewWriter(c.out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "NAME\tSTATUS\tRESOURCES\tVERSION\tUPDATED")
	for _, s := range stacks {
		fmt.Fprintf(tw, "%s\t%s\t%d\t%d\t%s\n", s.Name, s.Status, s.Resources, s.Version, s.UpdatedAt.Local().Format(time.RFC3339))
	}
	return tw.Flush()
}

func (c *cli) events(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("events", flag.ContinueOnError)
	stack := fs.String("s", "", "stack name (required)")
	followF := fs.Bool("follow", false, "keep streaming new events")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *stack == "" {
		return errors.New("-s stack is required")
	}
	cl := c.client()
	var after int64
	for {
		evs, err := cl.Events(ctx, *stack, after, 500)
		if err != nil {
			return err
		}
		for _, e := range evs {
			c.printEvent(e)
			after = e.ID
		}
		if len(evs) == 500 {
			continue
		}
		if !*followF {
			return nil
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(2 * time.Second):
		}
	}
}

func (c *cli) drift(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("drift", flag.ContinueOnError)
	stack := fs.String("s", "", "stack name (required)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *stack == "" {
		return errors.New("-s stack is required")
	}
	rep, err := c.client().DetectDrift(ctx, *stack)
	if err != nil {
		return err
	}
	tw := tabwriter.NewWriter(c.out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "LOGICAL ID\tTYPE\tSTATUS\tDETAILS")
	for _, r := range rep.Resources {
		color := "32"
		switch r.Status {
		case "MODIFIED", "DELETED":
			color = "31"
		case "NOT_CHECKED", "ERROR":
			color = "33"
		}
		details := r.Error
		var parts []string
		for _, d := range r.Diffs {
			parts = append(parts, fmt.Sprintf("%s: applied %s, live %s", d.Name, fmtValue(d.Old), fmtValue(d.New)))
		}
		if len(parts) > 0 {
			details = strings.Join(parts, "; ")
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", r.LogicalID, r.Type, c.paint(color, r.Status), details)
	}
	tw.Flush()
	if rep.Drifted {
		fmt.Fprintln(c.out, c.paint("31", "\nDrift detected. Redeploy the stack to restore the declared state."))
		return exitCode(2)
	}
	fmt.Fprintln(c.out, c.paint("32", "\nNo drift detected."))
	return nil
}

func (c *cli) validate(_ context.Context, args []string) error {
	fs := flag.NewFlagSet("validate", flag.ContinueOnError)
	file := fs.String("f", "", "template file, or - for stdin (required)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	_, t, err := readTemplate(*file)
	if err != nil {
		return err
	}
	if err := t.Validate(); err != nil {
		return err
	}
	g, _ := template.BuildGraph(t)
	fmt.Fprintf(c.out, "Template is valid: %d resource(s), %d parameter(s), %d output(s).\nDeployment order: %s\n",
		len(t.Resources), len(t.Parameters), len(t.Outputs), strings.Join(g.Order(), " → "))
	return nil
}

func (c *cli) types(ctx context.Context, args []string) error {
	ts, err := c.client().ResourceTypes(ctx)
	if err != nil {
		return err
	}
	tw := tabwriter.NewWriter(c.out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "TYPE\tDESCRIPTION")
	for _, t := range ts {
		fmt.Fprintf(tw, "%v\t%v\n", t["type"], t["description"])
	}
	return tw.Flush()
}
