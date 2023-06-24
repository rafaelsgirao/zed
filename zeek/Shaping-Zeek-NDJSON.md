# Shaping Zeek NDJSON

- [Summary](#summary)
- [Zeek Version/Configuration](#zeek-versionconfiguration)
- [Reference Shaper Contents](#reference-shaper-contents)
  * [Leading Type Definitions](#leading-type-definitions)
  * [Type Definitions Per Zeek Log `_path`](#type-definitions-per-zeek-log-_path)
  * [Mapping From `_path` Values to Types](#mapping-from-_path-values-to-types)
  * [Zed Pipeline](#zed-pipeline)
- [Invoking the Shaper From `zq`](#invoking-the-shaper-from-zq)
- [Importing Shaped Data Into Zui](#importing-shaped-data-into-zui)
- [Contact us!](#contact-us)

# Summary

As described in [Reading Zeek Log Formats](Reading-Zeek-Log-Formats.md),
logs output by Zeek in NDJSON format lose much of their rich data typing that
was originally present inside Zeek. This detail can be restored using a Zed
[shaper](https://zed.brimdata.io/docs/language/overview#10-shaping), such as the reference [`shaper.zed`](shaper.zed)
that can be found in this directory of the repository.

A full description of all that's possible with shapers is beyond the scope of
this doc. However, this example for shaping Zeek NDJSON is quite simple and
is described below.

# Zeek Version/Configuration

The fields and data types in the reference `shaper.zed` reflect the default
NDJSON-format logs output by Zeek releases up to the version number referenced
in the comments at the top of that file. They have been revisited periodically
as new Zeek versions have been released.

Most changes we've observed in Zeek logs between versions have involved only the
addition of new fields. Because of this, we expect the shaper should be usable
as is for Zeek releases older than the one most recently tested, since fields
in the shaper not present in your environment would just be filled in with
`null` values.

All attempts will be made to update this reference shaper in a timely manner
as new Zeek versions are released. However, if you have modified your Zeek
installation with [packages](https://packages.zeek.org/)
or other customizations, or if you are using a [Corelight Sensor](https://corelight.com/products/appliance-sensors/)
that produces Zeek logs with many fields and logs beyond those found in open
source Zeek, the reference shaper will not cover all the fields in your logs.
[As described below](#zed-pipeline), the reference shaper will assign
inferred types to such additional fields. By exploring your data, you can then
iteratively enhance your shaper to match your environment. If you need
assistance, please speak up on our [public Slack](https://www.brimdata.io/join-slack/).

# Reference Shaper Contents

The reference `shaper.zed` may seem large, but ultimately it follows a
fairly simple pattern that repeats across the many [Zeek log types](https://docs.zeek.org/en/master/script-reference/log-files.html).

## Leading Type Definitions

The top three lines define types that are referenced further below in the main
portion of the Zed shaper.

```
type port=uint16;
type zenum=string;
type conn_id={orig_h:ip,orig_p:port,resp_h:ip,resp_p:port};
```
The `port` and `zenum` types are described further in the [Zed/Zeek Data Type Compatibility](Data-Type-Compatibility.md)
doc. The `conn_id` type will just save us from having to repeat these fields
individually in the many Zeek record types that contain an embedded `id`
record.

## Type Definitions Per Zeek Log `_path`

The bulk of this Zed shaper consists of detailed per-field data type
definitions for each record in the default set of NDJSON logs output by Zeek.
These type definitions reference the types we defined above, such as `port`
and `conn_id`. The syntax for defining primitive and complex types follows the
relevant sections of the [ZSON Format](https://zed.brimdata.io/docs/formats/zson#2-the-zson-format)
specification.

```
...
type conn={_path:string,ts:time,uid:string,id:conn_id,proto:zenum,service:string,duration:duration,orig_bytes:uint64,resp_bytes:uint64,conn_state:string,local_orig:bool,local_resp:bool,missed_bytes:uint64,history:string,orig_pkts:uint64,orig_ip_bytes:uint64,resp_pkts:uint64,resp_ip_bytes:uint64,tunnel_parents:|[string]|,_write_ts:time}
type dce_rpc={_path:string,ts:time,uid:string,id:conn_id,rtt:duration,named_pipe:string,endpoint:string,operation:string,_write_ts:time}
...
```

> **Note:** See [the role of `_path` ](Reading-Zeek-Log-Formats.md#the-role-of-_path)
> for important details if you're using Zeek's built-in [ASCII logger](https://docs.zeek.org/en/current/scripts/base/frameworks/logging/writers/ascii.zeek.html)
> to generate NDJSON rather than the [JSON Streaming Logs](https://github.com/corelight/json-streaming-logs) package.

## Mapping From `_path` Values to Types

The next section is just simple [map](https://zed.brimdata.io/docs/formats/zed#24-map)
from the string values typically found
in the Zeek `_path` field to the name of one of the types we defined above.

```
const schemas = |{
  "analyzer": <analyzer>,
  "broker": <broker>,
  "capture_loss": <capture_loss>,
  "cluster": <cluster>,
  "config": <config>,
  "conn": <conn>,
  "dce_rpc": <dce_rpc>,
...
```

## Zed Pipeline

The Zed shaper ends with a pipeline that stitches together everything we've defined
so far.

```
yield nest_dotted(this)
| switch has(_path) (
  case true => switch (_path in schemas) (
    case true => yield {_original: this, _shaped: shape(schemas[_path])}
      | yield has_error(_shaped)
        ? error({msg: "shaper error(s): see inner error value(s) for details", _original, _shaped})
        : _shaped
    case false => yield error({msg: "shaper error: _path value " + _path + " not found in shaper config", _original: this})
  )
  case false => yield error({msg: "shaper error: input record lacks _path field", _original: this})
)
```

Picking this apart, it transforms each record as it's being read in several
steps:

1. [`nest_dotted()`](https://zed.brimdata.io/docs/language/functions/nest_dotted)
   reverses the Zeek NDJSON logger's "flattening" of nested
   records, e.g., how it populates a field named `id.orig_h` rather than
   creating a field `id` with sub-field `orig_h` inside it. Restoring the
   original nesting now gives us the option to reference the embedded record
   named `id` in the Zed language and access the entire 4-tuple of values, but
   still access the individual values using the same dotted syntax like
   `id.orig_h` when needed.

2. The [`switch`](https://zed.brimdata.io/docs/language/operators/switch)
   operators make sure a `_path` field is present and contains a value for
   which we have a type definition in our shaper. If either of these checks
   should fail the unshaped record is wrapped in an
   [error](https://zed.brimdata.io/docs/language/overview#63-first-class-errors)
   so it may be seen by a user for debug.

3. [`shape()`](https://zed.brimdata.io/docs/language/functions/shape) is
   applied to [`cast()`](https://zed.brimdata.io/docs/language/functions/cast),
   [`fill()`](https://zed.brimdata.io/docs/language/functions/fill), and
   [`order()`](https://zed.brimdata.io/docs/language/functions/order) the
   fields in the incoming record to what's in the matching type definition.

4. If [`has_error()`](https://zed.brimdata.io/docs/language/functions/has_error)
   finds error values at any level in the record after shaping was attempted
   (e.g., a field's value could not be successfully cast to a defined type)
   the unshaped record and error-decorated, partially-shaped record are both
   wrapped in an error so they may be seen by a user for debug.

Any fields that appear in the input record that are not present in the
type definition are kept and assigned an inferred data type. If you would
prefer to have such additional fields dropped (i.e., to maintain strict
adherence to the shape), append a call to the
[`crop()`](https://zed.brimdata.io/docs/language/functions/crop) function to the
Zed pipeline.

# Invoking the Shaper From `zq`

A shaper is typically invoked via the `-I` option of `zq`.

For example, if working in a directory containing many NDJSON logs, the
reference shaper can be applied to all the records they contain and
output them all in a single binary [ZNG](https://zed.brimdata.io/docs/formats/zng) file as
follows:

```
zq -I shaper.zed *.log > /tmp/all.zng
```

If you wish to apply the shaper and then perform additional
operations on the richly-typed records, the Zed query on the command line
should begin with a `|`, as this appends it to the pipeline at the bottom of
the shaper from the included file.

For example, to see a ZSON representaiton of just the errors that may have
come from attempting to shape all the logs in the current directory:

```
zq -Z -I shaper.zed '| has_error(this)' *.log
```

# Importing Shaped Data Into Zui

If you wish to browse your shaped data with [Zui](https://zui.brimdata.io/),
one way to accomplish this at the moment would be to use `zq` to convert
it to ZNG [as shown above](#invoking-the-shaper-from-zq) and then pipe that ZNG
into [`zed load`](https://zed.brimdata.io/docs/commands/zed#28-load) for
import to the Zed lake that runs behind Zui. See the
[Filesystem Paths](https://zui.brimdata.io/docs/support/Filesystem-Paths)
article for details on locating the Zed CLI binaries that are bundled with
Zui.

# Contact us!

If you're having difficulty, interested in shaping other data sources, or
just have feedback, please join our [public Slack](https://www.brimdata.io/join-slack/)
and speak up or [open an issue](https://github.com/brimdata/zed/issues/new/choose).
Thanks!
