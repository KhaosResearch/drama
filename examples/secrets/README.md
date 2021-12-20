#### Secrets management

Secrets uses a libsodium sealed box to help ensure that secrets are encrypted before they reach `drama` and remain encrypted until you use them in a workflow.

You can generate a new pair of private-public keys as follows:

```py
from nacl.public import PrivateKey
from nacl.encoding import Base64Encoder

import base64

def base64_to_bytes(key: str) -> bytes: 
    return base64.b64decode(key.encode('utf-8'))

sk = PrivateKey.generate()
pk = sk.public_key

sk_base64 = sk.encode(Base64Encoder).decode('utf8')
print("private key: " + sk_base64)

pk_base64 = pk.encode(Base64Encoder).decode('utf8')
print("public key: " + pk_base64)
```

#### Testing

> Workflows using relative module imports require setting **<kbd>API_DEBUG=1</kbd> in the environment** for security reasons.
> ```console 
> $ API_DEBUG=1 SECRETS_SK_KEY="qmADWyCD3i4JJQkmyy/Y8YSPYi0/rO+x+rvMUM0Kxs8=" poetry run drama worker --processes 1
> ```

From the root path of the project, use:

```console
$ poetry run python examples/secrets/run.py
```

