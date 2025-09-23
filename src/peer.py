# saved as peer.py
import Pyro5.api
import time
import threading
import argparse

# Ricart e Agrawala Algorithm
class Peer(object):
    states = ["RELEASED", "WANTED", "HELD"]
    peer_names = ["PeerA", "PeerB", "PeerC", "PeerD"]

    def __init__(self, name):
        self.name = name
        self.state = "RELEASED"
        self.timestamp = time.time()
        self.timer_since_request = 0
        self.queue = []
        self.peers = []  # List of other peer URIs
        self.heartbeat_interval = 5  # seconds
        self.last_heartbeat = time.time()
        self.last_heartbeat_times = {}
        self.approvals_received = {}

        if self.name not in self.peer_names:
            raise ValueError("Invalid peer name. Choose from: " + ", ".join(self.peer_names))
        self.peer_names.remove(self.name)

#### Pyro exposed methods ####

    @Pyro5.api.expose
    def request_sc(self, peer_timestamp, peer_uri):
        if self.state == "RELEASED":
            return True
        elif self.state == "WANTED":
            if (self.timestamp, Pyro5.api.current_context.client_sock.getpeername()) < (peer_timestamp, peer_uri):
                return True
            else:
                self.queue.append(peer_uri)
                return False
        elif self.state == "HELD":
            self.queue.append(peer_uri)
            return False

    @Pyro5.api.expose
    @Pyro5.api.oneway
    def set_heartbeat(self, peer_uri):
        print(f"Received heartbeat from {peer_uri}")
        self.last_heartbeat_times[peer_uri] = time.time()

    @Pyro5.api.expose
    @Pyro5.api.oneway
    def notify_queued(self, peer_uri):
        if self.approvals_received.get(peer_uri):
            self.approvals_received[peer_uri] = True
        self._enter_cs()

#### Internal Methods ####

    def request_cs(self):
        self.state = "HELD"
        self.timestamp = time.time()
        # Request permission from all peers
        for peer_uri in self.peers:
            peer = Pyro5.api.Proxy(peer_uri)
            approved = peer.request_sc(Pyro5.api.current_context.client_sock.getpeername())
            self.approvals_received[peer_uri] = approved

    def notify_heartbeat(self):
        current_time = time.time()
        if current_time - self.last_heartbeat >= self.heartbeat_interval:
            for peer_uri in self.peers:
                peer = Pyro5.api.Proxy(peer_uri)
                peer.set_heartbeat(Pyro5.api.current_context.client_sock.getpeername())
            self.last_heartbeat = current_time

    def check_inactive_peers(self):
        # Implement logic to check for inactive peers based on heartbeat timestamps
        pass

    def _enter_cs(self):
        # if for every peer in self.peers, self.approvals_received[peer] is True:
        if all(self.approvals_received.get(p, False) for p in self.peers):
            self.state = "HELD"
            self.approvals_received.clear()
            print(f"{self.name} has entered the critical section.")

    def _exit_cs(self):
        self.state = "RELEASED"
        print(f"{self.name} has exited the critical section.")
        # TO-DO: Check first if the peer_uri is active before notifying
        # Notify queued requests
        for peer_uri in self.queue:
            peer = Pyro5.api.Proxy(peer_uri)
            peer.notify_queued(Pyro5.api.current_context.client_sock.getpeername())
        self.queue.clear()
        self.approvals_received.clear() # Double check, just for sure

def main():
    parser = argparse.ArgumentParser(description="Aplicação Ricart-Agrawala com Pyro5")
    parser.add_argument("--name", type=str, required=True, help="Nome do processo [PeerA, PeerB, PeerC, PeerD]")

    args = parser.parse_args()

    daemon = Pyro5.server.Daemon()         # make a Pyro daemon
    ns = Pyro5.api.locate_ns()             # find the name server
    uri = daemon.register(Peer(args.name))   # register the greeting maker as a Pyro object
    ns.register(f"{args.name}", uri)   # register the object with a name in the name server

    print("Ready.")
    daemon.requestLoop()                   # start the event loop of the server to wait for calls

if __name__ == "__main__":
    main()