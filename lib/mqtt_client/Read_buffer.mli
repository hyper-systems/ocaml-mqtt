type t

(* val empty : unit -> t *)
val make : string -> t

(* val add_string : t -> string -> unit *)
val len : t -> int
val read : t -> int -> string
val read_string : t -> string
val read_uint8 : t -> int
val read_uint16 : t -> int
val read_all : t -> (t -> 'a) -> 'a list
