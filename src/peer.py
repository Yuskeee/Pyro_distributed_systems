# saved as peer.py
import Pyro5.api
import Pyro5.server
import time
import threading
import argparse
import sys
from threading import Timer, Lock

# Ricart e Agrawala Algorithm with required modifications
class Peer(object):
    states = ["RELEASED", "WANTED", "HELD"]
    peer_names = ["PeerA", "PeerB", "PeerC", "PeerD"]

    def __init__(self, name):
        self.name = name
        self.state = "RELEASED"
        self.timestamp = 0
        self.queue = []
        self.peers = {}  # Dict of peer_name -> URI
        self.active_peers = set()  # Set of active peer names
        
        # Heartbeat configuration
        self.heartbeat_interval = 5  # seconds
        self.heartbeat_timeout = 15  # seconds (3x interval)
        self.last_heartbeat_times = {}
        
        # Resource access control
        self.resource_timeout = 10  # seconds
        self.resource_timer = None
        
        # Request timeout configuration
        self.request_timeout = 10  # seconds
        self.pending_responses = {}
        self.response_timers = {}
        
        # Thread safety
        self.lock = Lock()
        
        if self.name not in self.peer_names:
            raise ValueError("Invalid peer name. Choose from: " + ", ".join(self.peer_names))
        
        # Initialize heartbeat tracking for other peers
        for peer_name in self.peer_names:
            if peer_name != self.name:
                self.last_heartbeat_times[peer_name] = time.time()

    #### Pyro exposed methods ####
    
    @Pyro5.api.expose
    def request_sc(self, peer_name, peer_timestamp):
        """Handle request for critical section access"""
        with self.lock:
            # Check if requesting peer is still active
            if not self._is_peer_active(peer_name):
                print(f"Ignoring request from inactive peer: {peer_name}")
                return False
            
            current_time = time.time()
            
            if self.state == "RELEASED":
                print(f"Granting immediate access to {peer_name}")
                return True
                
            elif self.state == "WANTED":
                # Compare timestamps and peer names for ordering
                if (peer_timestamp, peer_name) < (self.timestamp, self.name):
                    print(f"Granting priority access to {peer_name}")
                    return True
                else:
                    print(f"Queuing request from {peer_name}")
                    if peer_name not in self.queue:
                        self.queue.append(peer_name)
                    return False
                    
            elif self.state == "HELD":
                print(f"Queuing request from {peer_name} (resource in use)")
                if peer_name not in self.queue:
                    self.queue.append(peer_name)
                return False

    @Pyro5.api.expose
    @Pyro5.api.oneway
    def receive_heartbeat(self, peer_name):
        """Receive heartbeat from another peer"""
        with self.lock:
            self.last_heartbeat_times[peer_name] = time.time()
            self.active_peers.add(peer_name)
            print(f"Heartbeat received from {peer_name}")

    @Pyro5.api.expose
    @Pyro5.api.oneway
    def receive_release_notification(self, peer_name):
        """Receive notification that a peer has released the critical section"""
        with self.lock:
            if peer_name in self.pending_responses:
                self.pending_responses[peer_name] = True
                self._cancel_response_timer(peer_name)
                print(f"Received release notification from {peer_name}")
                self._check_enter_cs()

    #### Internal Methods ####
    
    def start_peer(self):
        """Initialize the peer and start background threads"""
        # Discover other peers
        self._discover_peers()
        
        # Start heartbeat thread
        heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        heartbeat_thread.start()
        
        # Start peer monitoring thread
        monitor_thread = threading.Thread(target=self._monitor_peers, daemon=True)
        monitor_thread.start()
        
        print(f"{self.name} started successfully")
        
    def request_cs(self):
        """Request access to critical section"""
        with self.lock:
            if self.state != "RELEASED":
                print(f"{self.name} is already requesting/holding the critical section")
                return
                
            self.state = "WANTED"
            self.timestamp = time.time()
            self.pending_responses.clear()
            
            print(f"{self.name} requesting critical section access...")
            
            # Send requests to all active peers
            active_peer_list = list(self.active_peers)
            if not active_peer_list:
                # No other active peers, can enter immediately
                self._enter_cs()
                return
                
            for peer_name in active_peer_list:
                if peer_name in self.peers:
                    self.pending_responses[peer_name] = False
                    self._send_request_with_timeout(peer_name)
            
            # Start checking for responses
            self._check_enter_cs()

    def _send_request_with_timeout(self, peer_name):
        """Send request to peer with timeout handling"""
        try:
            peer_uri = self.peers[peer_name]
            peer_proxy = Pyro5.api.Proxy(peer_uri)
            
            # Set up timeout timer
            timer = Timer(self.request_timeout, self._handle_request_timeout, [peer_name])
            self.response_timers[peer_name] = timer
            timer.start()
            
            # Send request
            response = peer_proxy.request_sc(self.name, self.timestamp)
            
            # Cancel timer and process response
            self._cancel_response_timer(peer_name)
            
            with self.lock:
                self.pending_responses[peer_name] = response
                if not response:
                    print(f"Request denied by {peer_name}")
                else:
                    print(f"Request approved by {peer_name}")
                self._check_enter_cs()
                
        except Exception as e:
            print(f"Error communicating with {peer_name}: {e}")
            self._handle_request_timeout(peer_name)

    def _handle_request_timeout(self, peer_name):
        """Handle timeout when waiting for response from peer"""
        with self.lock:
            if peer_name in self.active_peers:
                print(f"Timeout waiting for response from {peer_name}, marking as inactive")
                self.active_peers.discard(peer_name)
                
            if peer_name in self.pending_responses:
                del self.pending_responses[peer_name]
                
            self._cancel_response_timer(peer_name)
            self._check_enter_cs()

    def _cancel_response_timer(self, peer_name):
        """Cancel response timer for a peer"""
        if peer_name in self.response_timers:
            self.response_timers[peer_name].cancel()
            del self.response_timers[peer_name]

    def _check_enter_cs(self):
        """Check if all responses received and can enter critical section"""
        if self.state != "WANTED":
            return
            
        # Check if all pending responses are True or no pending responses
        if not self.pending_responses or all(self.pending_responses.values()):
            self._enter_cs()

    def _enter_cs(self):
        """Enter critical section"""
        self.state = "HELD"
        print(f"\n*** {self.name} ENTERED CRITICAL SECTION ***")
        print(f"*** Accessing shared resource... ***")
        
        # Set up automatic release timer
        self.resource_timer = Timer(self.resource_timeout, self._auto_release_resource)
        self.resource_timer.start()

    def release_cs(self):
        """Release critical section"""
        with self.lock:
            if self.state != "HELD":
                print(f"{self.name} is not holding the critical section")
                return
                
            self._exit_cs()

    def _exit_cs(self):
        """Exit critical section and notify queued peers"""
        if self.resource_timer:
            self.resource_timer.cancel()
            self.resource_timer = None
            
        self.state = "RELEASED"
        print(f"*** {self.name} EXITED CRITICAL SECTION ***\n")
        
        # Notify all queued peers
        queued_peers = self.queue.copy()
        self.queue.clear()
        
        for peer_name in queued_peers:
            if self._is_peer_active(peer_name) and peer_name in self.peers:
                try:
                    peer_proxy = Pyro5.api.Proxy(self.peers[peer_name])
                    peer_proxy.receive_release_notification(self.name)
                    print(f"Notified {peer_name} of resource release")
                except Exception as e:
                    print(f"Failed to notify {peer_name}: {e}")

    def _auto_release_resource(self):
        """Automatically release resource after timeout"""
        with self.lock:
            if self.state == "HELD":
                print(f"*** AUTO-RELEASING RESOURCE FOR {self.name} (TIMEOUT) ***")
                self._exit_cs()

    def _discover_peers(self):
        """Discover other peers from name server"""
        try:
            ns = Pyro5.api.locate_ns()
            
            for peer_name in self.peer_names:
                if peer_name != self.name:
                    try:
                        uri = ns.lookup(peer_name)
                        self.peers[peer_name] = uri
                        self.active_peers.add(peer_name)
                        print(f"Found peer {peer_name}: {uri}")
                    except:
                        print(f"Peer {peer_name} not found yet")
                        
        except Exception as e:
            print(f"Error discovering peers: {e}")

    def _heartbeat_loop(self):
        """Send periodic heartbeats to all peers"""
        while True:
            time.sleep(self.heartbeat_interval)
            self._send_heartbeats()

    def _send_heartbeats(self):
        """Send heartbeat to all known peers"""
        with self.lock:
            peers_to_contact = list(self.peers.items())
            
        for peer_name, peer_uri in peers_to_contact:
            try:
                peer_proxy = Pyro5.api.Proxy(peer_uri)
                peer_proxy.receive_heartbeat(self.name)
            except Exception as e:
                print(f"Failed to send heartbeat to {peer_name}: {e}")

    def _monitor_peers(self):
        """Monitor peer health and remove inactive ones"""
        while True:
            time.sleep(self.heartbeat_interval)
            current_time = time.time()
            
            with self.lock:
                inactive_peers = []
                for peer_name, last_time in self.last_heartbeat_times.items():
                    if current_time - last_time > self.heartbeat_timeout:
                        inactive_peers.append(peer_name)
                
                for peer_name in inactive_peers:
                    if peer_name in self.active_peers:
                        print(f"Marking {peer_name} as inactive")
                        self.active_peers.discard(peer_name)
                        
                        # Remove from pending responses if waiting
                        if peer_name in self.pending_responses:
                            del self.pending_responses[peer_name]
                            self._check_enter_cs()

    def _is_peer_active(self, peer_name):
        """Check if a peer is considered active"""
        current_time = time.time()
        if peer_name not in self.last_heartbeat_times:
            return False
        return current_time - self.last_heartbeat_times[peer_name] <= self.heartbeat_timeout

    def show_status(self):
        """Show current peer status"""
        with self.lock:
            print(f"\n=== {self.name} Status ===")
            print(f"State: {self.state}")
            print(f"Active peers: {list(self.active_peers)}")
            print(f"Queue: {self.queue}")
            print(f"Pending responses: {self.pending_responses}")
            print("========================\n")

def interactive_menu(peer):
    """Interactive menu for peer operations"""
    while True:
        print(f"\n=== {peer.name} Menu ===")
        print("1. Request Critical Section")
        print("2. Release Critical Section") 
        print("3. Show Status")
        print("4. Exit")
        
        try:
            choice = input("Choose option (1-4): ").strip()
            
            if choice == '1':
                peer.request_cs()
            elif choice == '2':
                peer.release_cs()
            elif choice == '3':
                peer.show_status()
            elif choice == '4':
                print("Exiting...")
                break
            else:
                print("Invalid option")
                
        except KeyboardInterrupt:
            print("\nExiting...")
            break
        except Exception as e:
            print(f"Error: {e}")

def main():
    parser = argparse.ArgumentParser(description="Ricart-Agrawala Algorithm with PyRO")
    parser.add_argument("--name", type=str, required=True, 
                       help="Process name [PeerA, PeerB, PeerC, PeerD]")
    args = parser.parse_args()

    # Create and register peer
    peer = Peer(args.name)
    daemon = Pyro5.server.Daemon()
    uri = daemon.register(peer)
    ns = Pyro5.api.locate_ns()             # find the name server
    
    try:
        ns.register(args.name, uri)
        print(f"Registered {args.name} with nameserver")
    except Exception as e:
        print(f"Registration error: {e}")
        # Try to remove existing registration and re-register
        try:
            ns.remove(args.name)
            ns.register(args.name, uri)
            print(f"Re-registered {args.name} with nameserver")
        except:
            print("Failed to re-register")
            sys.exit(1)

    # Start peer services
    peer.start_peer()
    
    print(f"\n{args.name} is ready!")
    print("Starting daemon in background...")
    
    # Start daemon in background thread
    daemon_thread = threading.Thread(target=daemon.requestLoop, daemon=True)
    daemon_thread.start()
    
    # Wait a bit for everything to initialize
    time.sleep(3)
    
    # Start interactive menu
    interactive_menu(peer)

if __name__ == "__main__":
    main()