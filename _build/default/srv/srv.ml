module T = Domainslib.Task

module Time = struct
  let string_of_local () =
    let z x = if x < 10 then Printf.sprintf "0%d" x else string_of_int x in
    let open Unix in
    let now = localtime @@ time () in
    Printf.sprintf "%s:%s:%s" (z now.tm_hour) (z now.tm_min) (z now.tm_sec)
  ;;
end

module Log = struct
  let error ?(ctx = "global") exn =
    Printexc.to_string exn |> Printf.sprintf "[%s] %s" ctx |> print_endline
  ;;

  let info ?(ctx = "global") msg = Printf.sprintf "[%s] %s" ctx msg |> print_endline
end

module Socket = struct
  let buf_len = 4096 (* 4KB *)

  let recv ?(stop = fun _ -> false) sock =
    let buf = Bytes.create buf_len in
    let return acc = Bytes.concat Bytes.empty @@ List.rev acc in
    let rec go acc =
      try
        let n = Unix.recv sock buf 0 buf_len [] in
        if n = 0
        then Ok (return acc)
        else (
          let chunk = Bytes.sub buf 0 n in
          let acc = chunk :: acc in
          if stop chunk then Ok (return acc) else go acc)
      with
      | exn -> Error exn
    in
    go []
  ;;

  let send sock buf =
    let len = Bytes.length buf in
    let rec go sent =
      if sent = len
      then Ok ()
      else (
        try
          let n = Unix.send sock buf sent (len - sent) [] in
          go (sent + n)
        with
        | exn -> Error exn)
    in
    go 0
  ;;
end

type config =
  { port : int
  ; backlog : int
  }

let default_config = { port = 3000; backlog = 128 }

let listen_tcp ?(config = default_config) () =
  let open Unix in
  let sock = socket PF_INET SOCK_STREAM 0 in
  setsockopt sock SO_REUSEADDR true;
  bind sock @@ ADDR_INET (inet_addr_any, config.port);
  listen sock config.backlog;
  Log.info @@ Printf.sprintf "listening on localhost:%d" config.port;
  sock
;;

let rec accept_loop pool handler sock =
  let () =
    match Unix.accept sock with
    | client, endpoint ->
      let _ = T.async pool (fun _ -> handler (client, endpoint)) in
      ()
    | exception exn -> Log.error ~ctx:"accept" exn
  in
  accept_loop pool handler sock
;;

let echo_handler (client, endpoint) =
  let stop chunk = String.ends_with ~suffix:"\n" @@ Bytes.to_string chunk in
  let close req =
    let tr = String.trim req in
    String.length tr = 0 || String.equal "close" tr
  in
  let ctx =
    Unix.(
      match endpoint with
      | ADDR_INET (addr, port) -> Printf.sprintf "%s:%d" (string_of_inet_addr addr) port
      | _ -> "request")
  in
  let echo req =
    Log.info ~ctx req;
    let resp =
      Bytes.of_string @@ Printf.sprintf "[%s] %s" (Time.string_of_local ()) req
    in
    match Socket.send client resp with
    | Ok () -> ()
    | Error exn -> Log.error ~ctx:"echo send" exn
  in
  let rec go () =
    match Socket.recv ~stop client with
    | Error exn -> Log.error ~ctx:"echo recv" exn
    | Ok buf ->
      let req = Bytes.to_string buf in
      if close req then Unix.close client else echo req |> go
  in
  go ()
;;

let () =
  let num_domains = Domain.recommended_domain_count () in
  let pool = T.setup_pool ~num_domains:(num_domains - 1) () in
  accept_loop pool echo_handler @@ listen_tcp ()
;;
