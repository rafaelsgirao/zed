### Aggregate Function

&emsp; **collect** &mdash; aggregate values into array

### Synopsis
```
collect(any) -> [any]
```
### Description

The _collect_ aggregate function organizes its input into an array.
If the input values vary in type, the return type will be an array
of union of the types encountered.

### Examples

Simple sequence collected into an array:
```mdtest-command
echo '1 2 3 4' | zq -z 'collect(this)' -
```
=>
```mdtest-output
{collect:[1,2,3,4]}
```

Continuous collection over a simple sequence:
```mdtest-command
echo '1 2 3 4' | zq -z 'yield collect(this)' -
```
=>
```mdtest-output
[1]
[1,2]
[1,2,3]
[1,2,3,4]
```
Mixed types create a union type for the array elements:
```mdtest-command
echo '1 2 3 4 "foo"' | zq -z 'collect(this)' -
```
=>
```mdtest-output
{collect:[1((int64,string)),2((int64,string)),3((int64,string)),4((int64,string)),"foo"((int64,string))]}
```