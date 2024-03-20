# OCaml MQTT Client

[![OCaml-CI Build Status](https://img.shields.io/endpoint?url=https%3A%2F%2Fci.ocamllabs.io%2Fbadge%2Fhyper-systems%2Focaml-mqtt%2Fmaster&logo=ocaml)](https://ci.ocamllabs.io/github/hyper-systems/ocaml-mqtt)

This library implements the client MQTT v3 protocol.

* [API Documentation](https://hyper-systems.github.io/ocaml-mqtt/mqtt-client/Mqtt_client)
* [Issues](https://github.com/hyper-systems/ocaml-mqtt/issues)

> Originally forked from https://github.com/j0sh/ocaml-mqtt.

## Quickstart

In your dune project add the following dependencies to your dune file:

```lisp
(executable
  (name My_app)
  (public_name my_app)
  (libraries mqtt-client lwt)
  (preprocess (pps lwt_ppx)))
```

## Examples

Here is a basic example of a subscriber:

```reason
module C = Mqtt_client

let host = "127.0.0.1"
let port = 1883

let sub_example () =
  let on_message ~topic payload = Lwt_io.printlf "%s: %s" topic payload in
  let%lwt () = Lwt_io.printl "Starting subscriber..." in
  let%lwt client = C.connect ~on_message ~id:"client-1" ~port [ host ] in
  C.subscribe [ ("topic-1", C.Atmost_once) ] client

let pub_example () =
  let%lwt () = Lwt_io.printl "Starting publisher..." in
  let%lwt client = C.connect ~id:"client-1" ~port [ host ] in
  let rec loop () =
    let%lwt () = Lwt_io.printl "Publishing..." in
    let%lwt line = Lwt_io.read_line Lwt_io.stdin in
    let%lwt () = C.publish ~qos:C.Atleast_once ~topic:"topic-1" line client in
    let%lwt () = Lwt_io.printl "Published." in
    loop ()
  in
  loop ()

let () = Lwt_main.run (sub_example ())
```
