
  let src = Logs.Src.create "tests.test_keep_alive" ~doc:"Keepalive timeout test"
  module Log = (val Logs_lwt.src_log src : Logs_lwt.LOG)

let (>>=) = Lwt.bind
let (<&>) = Lwt.(<&>)


module Client = struct
  let rec read_messages stream =
    Lwt_stream.get stream >>= function
    | None -> Log.info (fun log -> log "Client: Message stream empty.")
    | Some (t, p) ->
      let%lwt () = Log.info (fun log -> log "Client: Got message «%s: %s»." t p) in
      read_messages stream

  let start ?(host = "localhost") () =
    let%lwt () = Log.info (fun log -> log "Client: Starting...") in
    let opt = Mqtt.Client.options ~keep_alive:5 ~ping_timeout:3. ~flags:[] ~clientid:"Client-1" () in
    let%lwt c = Mqtt.Client.connect ~opt host in
    let%lwt () = Mqtt.Client.subscribe c ["topic-1", Mqtt.Atmost_once] in
    let%lwt () = Log.info (fun log -> log "Client: Subscribed...") in
    let stream = Mqtt.Client.messages c in
    read_messages stream
end


module Server = struct
  exception Stop

  let on_listen _address (in_channel, out_channel) =
    let%lwt () = Log.info (fun log -> log "Server: Connection estabilished.") in
    let ping_count = ref 0 in
    let ping_limit = 3 in

    let%lwt () =
      match%lwt Mqtt.read_packet in_channel with
      | (_, Connect _) ->
        let connack = Mqtt.Packet.Encoder.connack ~session_present:false Accepted in
        Lwt_io.write out_channel connack
      | _ -> Lwt.fail (Failure "Server: Expected connect packet.") in

    let rec loop () =
      let%lwt () =
        match%lwt Mqtt.read_packet in_channel with
        | (_, Publish _) ->
          Log.info (fun log -> log "Server: Publish request.")
        | (_, Subscribe (id, qos_list)) ->
          let%lwt () = Log.info (fun log -> log "Server: Subscribe request.") in
          let connack = Mqtt.suback id (List.map snd qos_list) in
          Lwt_io.write out_channel connack

        | (_, Disconnect) ->
          let%lwt () = Log.info (fun log -> log "Server: Disconnect request.") in
          Lwt.fail Stop
        | (_, Pingreq) when !ping_count >= ping_limit ->
          Log.info (fun log -> log "Server: ping request, ignoring...")
        | (_, Pingreq) ->
          let%lwt () = Log.info (fun log -> log "Server: Ping request, sending ping response...") in
          Lwt_io.write out_channel (Mqtt.pingresp ()) >>= fun () ->
          incr ping_count;
          Lwt.return_unit

        | _ -> Lwt.fail (Failure "Server: Unknown packet.") in
      loop ()
    in
    Lwt.catch loop
      (function
       | Stop -> Log.info (fun log -> log "Server: Stopped (idle connection).")
       | exn -> Lwt.fail exn)


  let addr host port =
    Lwt_unix.gethostbyname host >>= fun hostent ->
    let inet_addr = hostent.Unix.h_addr_list.(0) in
    Unix.ADDR_INET (inet_addr, port) |> Lwt.return


  let start ?(host = "localhost") ?(port = 1883) () =
    addr host port >>= fun a ->
    let%lwt () = Log.info (fun log -> log "Server: Starting...") in
    Lwt_io.establish_server_with_client_address ~backlog:1000 a on_listen
end



let () =
  Logs.set_reporter (Logs_fmt.reporter ());
  Logs.set_level (Some Logs.Debug);

  Lwt_main.run begin
    match Sys.argv with
    | [|_; "server"|] ->
      ignore (Server.start ());
      Lwt.pick []
    | [|_; "client"|] -> Client.start ()
    | _ -> Lwt_io.printlf "usage: %s <client|server>" Sys.argv.(0)
  end

