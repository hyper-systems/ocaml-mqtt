# OCaml MQTT Clint

This library implements the client MQTT v3 protocol.

* [API Documentation](https://odis-labs.github.io/ocaml-mqtt/mqtt-client/Mqtt_client)
* [Issues](https://github.com/odis-labs/ocaml-mqtt/issues)

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
  let%lwt client = C.connect(~id="client-1", ~port, [host]);
  let%lwt () = C.subscribe([("topic-1", C.Atmost_once)], client);
  let stream = C.messages(client);
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
