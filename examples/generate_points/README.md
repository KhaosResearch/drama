#### Testing

> Workflows using relative module imports require setting **<kbd>API_DEBUG=1</kbd> in the environment** for security reasons:
> ```console 
> $ API_DEBUG=1 poetry run drama worker --processes 1
> ```

From the root path of the project, use:

```console
$ poetry run python examples/generate_points/run.py
```
