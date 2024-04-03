module BE = EndianBytes.BigEndian

type t = { mutable pos : int; mutable buf : bytes }

let create () = { pos = 0; buf = Bytes.of_string "" }

let add_string rb str =
  let str = Bytes.of_string str in
  let curlen = Bytes.length rb.buf - rb.pos in
  let strlen = Bytes.length str in
  let newlen = strlen + curlen in
  let newbuf = Bytes.create newlen in
  Bytes.blit rb.buf rb.pos newbuf 0 curlen;
  Bytes.blit str 0 newbuf curlen strlen;
  rb.pos <- 0;
  rb.buf <- newbuf

let make str =
  let rb = create () in
  add_string rb str;
  rb

let len rb = Bytes.length rb.buf - rb.pos

let read rb count =
  let len = Bytes.length rb.buf - rb.pos in
  if count < 0 || len < count then raise (Invalid_argument "buffer underflow");
  let ret = Bytes.sub rb.buf rb.pos count in
  rb.pos <- rb.pos + count;
  Bytes.to_string ret

let read_uint8 rb =
  let str = rb.buf in
  let slen = Bytes.length str - rb.pos in
  if slen < 1 then raise (Invalid_argument "string too short");
  let res = BE.get_uint8 str rb.pos in
  rb.pos <- rb.pos + 1;
  res

let read_uint16 rb =
  let str = rb.buf in
  let slen = Bytes.length str - rb.pos in
  if slen < 2 then raise (Invalid_argument "string too short");
  let res = BE.get_uint16 str rb.pos in
  rb.pos <- rb.pos + 2;
  res

let read_string rb = read_uint16 rb |> read rb

let read_all rb f =
  let rec loop res = if len rb <= 0 then res else loop (f rb :: res) in
  loop []
