# connection.py
from __future__ import annotations

import socket
import threading
import queue
import struct
import time
from typing import Callable, Optional, Tuple, Set


class Connection:
    """
    Connessione bidirezionale semplice tra due dispositivi sulla stessa LAN.

    Ogni istanza:
      - Ascolta su (server_ip, server_port) per ricevere dati dal peer.
      - Mantiene (e ri-mantiene) una connessione client verso (client_ip, client_port)
        usata per inviare dati al peer.

    API minima:
      - start(on_message: Callable[[str], None]) -> None
      - send(text: str) -> None
      - stop() -> None
      - join(timeout: Optional[float] = None) -> None

    Esempio di parametrizzazione su due PC:
      PC_A: Connection(client_ip=B_ip, client_port=5001, server_ip=A_ip, server_port=5000)
      PC_B: Connection(client_ip=A_ip, client_port=5000, server_ip=B_ip, server_port=5001)
    """

    # --- costanti framing (lunghezza 4 byte, big-endian) ---
    _HDR_FMT = "!I"
    _HDR_LEN = struct.calcsize(_HDR_FMT)

    def __init__(
        self,
        client_ip: str,
        client_port: int,
        server_ip: str,
        server_port: int,
        *,
        reconnect_interval_s: float = 1.0,
        recv_spawn_thread: bool = True,
        tcp_nodelay: bool = True,
        keepalive: bool = True,
        name: Optional[str] = None,
    ) -> None:
        self.remote_addr: Tuple[str, int] = (client_ip, client_port)  # dove invio
        self.local_addr: Tuple[str, int] = (server_ip, server_port)   # dove ascolto

        self._reconnect_interval_s = reconnect_interval_s
        self._recv_spawn_thread = recv_spawn_thread
        self._tcp_nodelay = tcp_nodelay
        self._keepalive = keepalive
        self._name = name or f"{server_ip}:{server_port}→{client_ip}:{client_port}"

        # Stato / sincronizzazione
        self._stop_event = threading.Event()
        self._on_message: Optional[Callable[[str], None]] = None

        # Server in ascolto
        self._listener_sock: Optional[socket.socket] = None
        self._accept_thread: Optional[threading.Thread] = None
        self._incoming_sockets: Set[socket.socket] = set()
        self._incoming_lock = threading.Lock()

        # Client in uscita
        self._out_sock: Optional[socket.socket] = None
        self._out_lock = threading.Lock()
        self._connect_thread: Optional[threading.Thread] = None
        self._send_thread: Optional[threading.Thread] = None
        self._send_queue: "queue.Queue[str]" = queue.Queue()

    # ---------------------- API PUBBLICA ----------------------

    def start(self, on_message: Callable[[str], None]) -> None:
        """
        Avvia listener (server) e connettore (client). Ritorna subito.
        on_message verrà invocata per ogni messaggio ricevuto (stringa).
        """
        if self._on_message is not None:
            raise RuntimeError("Connection già avviata")

        self._on_message = on_message
        self._stop_event.clear()

        # Avvia server (accept loop)
        self._accept_thread = threading.Thread(
            target=self._accept_loop,
            name=f"ConnAccept[{self._name}]",
            daemon=True,
        )
        self._accept_thread.start()

        # Avvia client (connect + maintainer) e thread di invio
        self._connect_thread = threading.Thread(
            target=self._connect_loop,
            name=f"ConnConnect[{self._name}]",
            daemon=True,
        )
        self._connect_thread.start()

        self._send_thread = threading.Thread(
            target=self._send_loop,
            name=f"ConnSend[{self._name}]",
            daemon=True,
        )
        self._send_thread.start()

    def send(self, text: str) -> None:
        """Invia una stringa al peer (messa in coda e spedita appena possibile)."""
        if self._on_message is None:
            raise RuntimeError("Chiama start(on_message) prima di send()")
        if self._stop_event.is_set():
            raise RuntimeError("Connection già fermata")
        # Mettiamo in coda; l'invio reale lo fa _send_loop
        self._send_queue.put_nowait(text)

    def stop(self) -> None:
        """Ferma connessioni e thread. È safe chiamarla più volte."""
        self._stop_event.set()

        # Chiudi listener e tutte le incoming
        try:
            if self._listener_sock:
                self._listener_sock.close()
        except Exception:
            pass

        with self._incoming_lock:
            for s in list(self._incoming_sockets):
                try:
                    s.close()
                except Exception:
                    pass
            self._incoming_sockets.clear()

        # Chiudi out socket
        with self._out_lock:
            if self._out_sock:
                try:
                    self._out_sock.close()
                except Exception:
                    pass
                self._out_sock = None

    def join(self, timeout: Optional[float] = None) -> None:
        """
        Blocca il chiamante finché la connessione non viene fermata (o fino al timeout).
        Utile per replicare il pattern .join() dei listener.
        """
        start = time.time()
        while any(t and t.is_alive() for t in (self._accept_thread, self._connect_thread, self._send_thread)):
            if timeout is not None and (time.time() - start) >= timeout:
                return
            time.sleep(0.05)

    # ---------------------- LOOP SERVER ----------------------

    def _accept_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                lsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                if self._keepalive:
                    lsock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                lsock.bind(self.local_addr)
                lsock.listen(5)
                lsock.settimeout(1.0)  # per poter controllare periodicamente lo stop
                self._listener_sock = lsock

                # Accept loop
                while not self._stop_event.is_set():
                    try:
                        conn, addr = lsock.accept()
                    except socket.timeout:
                        continue
                    except OSError:
                        break  # listener chiuso in stop()

                    if self._tcp_nodelay:
                        try:
                            conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                        except Exception:
                            pass

                    with self._incoming_lock:
                        self._incoming_sockets.add(conn)

                    # Thread di ricezione per questa socket
                    t = threading.Thread(
                        target=self._recv_loop,
                        args=(conn, addr),
                        name=f"ConnRecv[{self._name}<-{addr[0]}:{addr[1]}]",
                        daemon=True,
                    )
                    t.start()

                # uscita dal while: chiusura listener
            except OSError:
                # binding/porta occupata, riprova tra poco se non stoppati
                if not self._stop_event.is_set():
                    time.sleep(1.0)
            finally:
                if self._listener_sock:
                    try:
                        self._listener_sock.close()
                    except Exception:
                        pass
                    self._listener_sock = None

        # loop terminato
        return

    def _recv_loop(self, conn: socket.socket, addr: Tuple[str, int]) -> None:
        try:
            conn.settimeout(1.0)
            buf = b""
            while not self._stop_event.is_set():
                # Leggi header (4 byte)
                hdr = self._read_exactly(conn, self._HDR_LEN)
                if hdr is None:
                    break  # socket chiusa dal peer
                (length,) = struct.unpack(self._HDR_FMT, hdr)
                if length == 0:
                    payload = b""
                else:
                    payload = self._read_exactly(conn, length)
                    if payload is None:
                        break

                try:
                    text = payload.decode("utf-8")
                except UnicodeDecodeError:
                    # Se arriva spazzatura, ignora questo messaggio
                    continue

                # Esegui callback
                if self._recv_spawn_thread:
                    threading.Thread(
                        target=self._safe_on_message,
                        args=(text,),
                        name=f"OnMessage[{self._name}]",
                        daemon=True,
                    ).start()
                else:
                    self._safe_on_message(text)
        except Exception:
            pass
        finally:
            with self._incoming_lock:
                if conn in self._incoming_sockets:
                    self._incoming_sockets.discard(conn)
            try:
                conn.close()
            except Exception:
                pass

    # ---------------------- LOOP CLIENT / SEND ----------------------

    def _connect_loop(self) -> None:
        """Mantiene una connessione client verso remote_addr, con auto-riconnessione."""
        while not self._stop_event.is_set():
            with self._out_lock:
                if self._out_sock:
                    # già connessi: dormi e ricontrolla
                    pass
                else:
                    try:
                        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        if self._tcp_nodelay:
                            try:
                                s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                            except Exception:
                                pass
                        if self._keepalive:
                            s.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                        s.settimeout(3.0)
                        s.connect(self.remote_addr)
                        s.settimeout(None)  # blocking per sendall/recv
                        self._out_sock = s
                    except OSError:
                        # non raggiungibile, riprova tra poco
                        self._out_sock = None

            # ritmo del maintainer
            time.sleep(self._reconnect_interval_s)

        # uscita quando stop_event è settato

    def _send_loop(self) -> None:
        """Legge dalla coda e invia, aspettando che la connessione sia disponibile."""
        while not self._stop_event.is_set():
            try:
                text = self._send_queue.get(timeout=0.2)
            except queue.Empty:
                continue

            data = text.encode("utf-8")
            header = struct.pack(self._HDR_FMT, len(data))

            # attende una connessione disponibile
            while not self._stop_event.is_set():
                with self._out_lock:
                    s = self._out_sock
                if s is None:
                    time.sleep(self._reconnect_interval_s)
                    continue

                try:
                    s.sendall(header + data)
                    break  # messaggio spedito
                except OSError:
                    # connessione caduta: chiudi e lascia che _connect_loop la ricrei
                    with self._out_lock:
                        try:
                            if self._out_sock:
                                self._out_sock.close()
                        except Exception:
                            pass
                        self._out_sock = None
                    # riprova sul prossimo giro
                    time.sleep(self._reconnect_interval_s)

        # svuota eventuale coda residua in chiusura (opzionale: potresti volerla ignorare)
        return

    # ---------------------- UTILS ----------------------

    def _read_exactly(self, s: socket.socket, n: int) -> Optional[bytes]:
        """Legge esattamente n byte o ritorna None se la socket si chiude."""
        chunks = []
        remaining = n
        while remaining > 0 and not self._stop_event.is_set():
            try:
                chunk = s.recv(remaining)
            except socket.timeout:
                continue
            except OSError:
                return None
            if not chunk:
                return None
            chunks.append(chunk)
            remaining -= len(chunk)
        if remaining == 0:
            return b"".join(chunks)
        return None

    def _safe_on_message(self, text: str) -> None:
        cb = self._on_message
        if cb is None:
            return
        try:
            cb(text)
        except Exception:
            # Evita che un'eccezione utente uccida i thread di ricezione
            pass
