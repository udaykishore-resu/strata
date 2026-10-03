# Template format

Templates are JSON documents. The construct library generates them, but they are designed to be readable and writable by hand.

```json
{
  "formatVersion": "2026-10-01",
  "description": "Orders platform",
  "parameters": {
    "Env": {"type": "string", "default": "dev", "allowed": ["dev", "prod"]}
  },
  "resources": {
    "Events": {"type": "gcp:pubsub:Topic", "properties": {"labels": {"env": {"param": "Env"}}}},
    "Worker": {
      "type": "gcp:pubsub:Subscription",
      "properties": {"topic": {"ref": "Events"}, "ackDeadlineSeconds": 30},
      "deletionPolicy": "Retain"
    }
  },
  "outputs": {
    "TopicId": {"value": {"getAtt": ["Events", "id"]}, "description": "Full topic name"}
  }
}
```

## Sections

| Field | Required | Notes |
|---|---|---|
| `formatVersion` | yes | Must be `2026-10-01`. |
| `description` | no | |
| `parameters` | no | `type` is `string`, `number` or `bool`; optional `default`, `allowed`, `description`. Parameters without a default are required at deploy time. |
| `resources` | yes | Map of logical ID (alphanumeric, starts with a letter) to resource. |
| `outputs` | no | Values resolved after a successful deploy and stored on the stack. |

A resource has a `type` (`provider:service:Kind`), `properties`, an optional `dependsOn` list for ordering without a data reference, and an optional `deletionPolicy` (`Delete`, the default, or `Retain`).

Unknown fields are rejected everywhere, so typos fail at plan time rather than being ignored.

## Intrinsics

An intrinsic is an object with exactly one key.

| Intrinsic | Value |
|---|---|
| `{"ref": "Logical"}` | The resource's physical ID (bucket name, topic ID, service ID, account ID). |
| `{"getAtt": ["Logical", "attr"]}` | A resource attribute; see each type's attributes in [resource-types.md](resource-types.md). |
| `{"param": "Name"}` | A parameter, or a pseudo parameter: `strata:project`, `strata:region`, `strata:stack`. |
| `{"join": ["sep", [part, ...]]}` | Concatenation; parts may be intrinsics. |

`ref` and `getAtt` create dependency edges. Cycles are rejected.

## Names and replacement

Most types have a name property (`name`, `accountId`, `secretId`, …). If you omit it, Strata generates `<stack>-<logicalid>-<random>` within the type's length limit. **Prefer generated names:** a change to an immutable property then replaces the resource safely (new one first, old one deleted after the deploy succeeds). If you set a custom name and change an immutable property without changing the name, the plan fails with an explanation instead of attempting an impossible replacement.

Labeled types receive `strata-stack` and `strata-lid` labels automatically. They identify ownership so an interrupted create can be safely adopted on retry.

## Change actions

| Action | Meaning |
|---|---|
| `Create` | New logical ID. |
| `Update` | Mutable properties changed; updated in place. |
| `Replace` | An immutable (`forceNew`) property or the type changed, possibly because a referenced resource is being replaced. |
| `Delete` | Logical ID removed from the template; deleted in the cleanup phase. |
| `NoOp` | No change. |

A diff value shown as `(known after apply)` depends on a resource that does not exist yet.
