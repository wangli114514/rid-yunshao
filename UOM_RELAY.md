# UOM CONNECT relay

The cloud host currently times out when it connects directly to
`uom.caac.gov.cn:443`, while the operator workstation can reach the UOM page.
This repository includes a small local HTTP CONNECT relay for that case.

The relay is intentionally narrow:

- it binds to `127.0.0.1` by default;
- it accepts only `CONNECT uom.caac.gov.cn:443`;
- it does not resolve or connect to a client-supplied host;
- it never logs headers, query strings, tokens, or tunnel bytes;
- it has bounded header size, idle time, and concurrent connections.

It is not a general-purpose proxy. The UOM token remains in the cloud
service's environment and is carried inside the encrypted SSH connection.

## Start on Windows

From the repository root:

```powershell
python uom_connect_relay.py --bind 127.0.0.1 --port 19090
```

The default loopback listener is `127.0.0.1:19090`. Use
`-UomRelayPort 19100` if that port is busy. Stop it with:

停止时结束该进程即可。

The action does not read `RID_UOM_WMS_TOKEN`, and it does not stop or restart
the demo simulator.

## SSH reverse forwarding

The workstation needs a persistent SSH connection to the cloud host. A basic
reverse forward is:

```powershell
ssh -N -o ExitOnForwardFailure=yes -o ServerAliveInterval=30 -o ServerAliveCountMax=3 `
  -R 127.0.0.1:19090:127.0.0.1:19090 user@YOUR_CLOUD_HOST
```

The command above makes the relay available only on the cloud host loopback.
The Docker bridge cannot normally reach that loopback address. Before using
the proxy from `rid-monitor`, expose the reverse-forward listener only on the
Docker host gateway (not on the public interface), or use a host-side firewall
rule that permits the Docker bridge subnet and denies other sources. The SSH
daemon may require `GatewayPorts clientspecified` for a non-loopback bind.

After that host-side step, set this **cloud-only** environment variable in
	`.env` to the host-gateway address and forwarded port, for
example:

```dotenv
RID_UOM_WMS_PROXY=http://172.17.0.1:19090
```

Do not place a UOM token in the proxy URL. `RID_UOM_WMS_PROXY` is optional; an
empty value keeps the existing direct mode. The application status endpoint
reports only `mode`, `host`, and `port`, never the complete URL or credential.

## Verify locally

The relay tests use a local fake upstream and do not contact UOM:

```powershell
	python -m unittest -v test_uom_connect_relay.py
	python -m py_compile uom_connect_relay.py
```

A successful relay test proves the allow-list and byte-copy behavior, not that
the temporary UOM token is still valid. Test a real WMS tile only after the
SSH path and cloud proxy address are configured.
