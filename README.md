# OCaml MQTT Client

[![OCaml-CI Build Status](https://img.shields.io/endpoint?url=https%3A%2F%2Fci.ocamllabs.io%2Fbadge%2Focurrent%2Focaml-ci%2Fmaster&logo=ocaml)](https://ci.ocamllabs.io/github/hyper-systems/ocaml-mqtt)

This library implements the client MQTT v3 protocol.

* [API Documentation](https://hyper-systems.github.io/ocaml-mqtt/mqtt-client/Mqtt_client)
* [Issues](https://github.com/hyper-systems/ocaml-mqtt/issues)

Originally forked from https://github.com/j0sh/ocaml-mqtt.

## Quickstart

To install ocaml-mqtt in an esy project, add the following dependency to your package.json file:

```json
"dependencies": {
  "ocaml-mqtt": "odis-labs/ocaml-mqtt#3a97e01"
}
```

In your dune project add the following dependencies to your dune file:


```lisp
(executable
  (name My_app)
  (public_name my-app)
  (libraries mqtt-client lwt)
  (preprocess (pps lwt_ppx)))
```

## Examples

Here is a basic example of a subscriber:

```reason
open Printf;

let sub_example = () => {
  let%lwt client = Mqtt_client.connect(~id="client-1", ~port, [host]);
  let%lwt () = Mqtt_client.subscribe([("topic-1", Mqtt_client.Atmost_once)], client);
  let stream = Mqtt_client.messages(client);
  let rec loop = () => {
    let%lwt msg = Lwt_stream.get(stream);
    switch(msg) {
      | None => Lwt_io.printl("STREAM FINISHED")
      | Some((topic, payload)) =>
        printlf("%s: %s", topic, payload);
        loop()
    };
  };
  loop();
};
```
