
type t
(** Represents an MQTT client.
    
    To create a new client use {!val:Mqtt_client.connect}. *)


(** Client authentication credentials.
    
    MQTT supports two authentication methods: username with password, or
    username only.
    
    The credentials will be sent in plain text, unless TLS is used.
    See {{!val:Mqtt_client.connect}connection options} for more information. *)
type credentials =
  | Credentials of string * string
  | Username of string


(** Quality of Service level.

    Defines the guarantee of delivery for messages. *)
type qos =
  | Atmost_once
  | Atleast_once
  | Exactly_once


val connect
   : ?id:string
  -> ?tls_ca:string
  -> ?credentials:credentials
  -> ?will:string * string
  -> ?clean_session:bool
  -> ?keep_alive:int
  -> ?ping_timeout:float
  -> ?on_error:(t -> exn -> unit Lwt.t)
  -> ?port:int
  -> string list
  -> t Lwt.t
(** Connects to the MQTT broker.

    Multiple hosts can be provided in case the broker supports failover.
    The client will attempt to connect to each one of hosts sequentially until
    one of them is successful.

    [on_error] can be provided to handle errors during client's execution.
    By default all internal exceptions will be raised with [Lwt.fail].
    
    {i Note:} Reconnection logic is not implemented currently.

    {[
    let broker_hosts = ["host-1", "host-2"];
    let%lwt client = Mqtt_client.connect(~id="my-client", ~port=1883, broker_hosts);
    ]} *)


val disconnect : t -> unit Lwt.t
(** Disconnects the client from the MQTT broker.

    {[
    let%lwt () = Mqtt_client.disconnect(client);
    ]} *)


val publish
   : ?dup:bool
  -> ?qos:qos
  -> ?retain:bool
  -> topic:string
  -> string
  -> t
  -> unit Lwt.t
(** Publish a message with payload to a given topic.

    {[
    let payload = "Hello world";
    let%lwt () = Mqtt_client.publish(~topic="news", payload, client);
    ]} *)


val subscribe
   : ?dup:bool
  -> ?qos:qos
  -> ?retain:bool
  -> ?id:int
  -> (string * qos) list
  -> t
  -> unit Lwt.t
(** Subscribes the client to a list of topics.

    {[
    let topics = [
      ("news/fashion", Mqtt_client.Atmost_once),
      ("news/science", Mqtt_client.Atleast_once)
    ];
    let%lwt () = Mqtt_client.subscribe(topics, client);
    ]} *)

val messages : t -> (string * string) Lwt_stream.t
(** A stream of messages for all subscriptions.

    The stream contains pairs with a topic and the message payload.

    {[
    let messages = Mqtt_client.messages(client);
    let%lwt () = Lwt_stream.iter(
      (topic, payload) => print_endline(payload),
      messages
    );
    ]} *)