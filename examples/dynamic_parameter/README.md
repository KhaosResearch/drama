#### Testing

From the root path of the project, use:

```console
$ poetry run python examples/dynamic_parameter/run.py
```

You can intercept the `DynamicParameter` component by POSTing a new message as follows:

```shell
curl -X 'POST' \
  'http://0.0.0.0:8005/api/v2/workflow/topic?id={WOKDLOW_ID}&component=ComponentParameter&message=myvalue' \
  -H 'accept: application/json' \
  -d ''
```