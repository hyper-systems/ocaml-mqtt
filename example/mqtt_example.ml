let (>>=) = Lwt.bind

let sub_example () =
    let open Mqtt.Mqtt.MqttClient in
    let read_subs stream =
        let rec loop unit =
        unit >>= fun () -> Lwt_stream.get stream >>= function
            | None -> Lwt_io.printl "STREAM FINISHED"
            | Some (t, p) -> Lwt_io.printlf "%s: %s" t p |> loop in
        loop Lwt.return_unit in
    connect "localhost" >>= fun client ->
    subscribe client [("foospace", Mqtt.Mqtt.Atmost_once)] >>= fun () ->
    sub_stream client |> read_subs

let pub_example () =
    let open Mqtt.Mqtt.MqttClient in
    connect "localhost" >>= fun cxn ->
    publish cxn "foospace" "isn't this awesome?"

let () = Lwt_main.run (sub_example ())
