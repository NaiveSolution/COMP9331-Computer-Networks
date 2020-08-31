# USING PYTHON 3.6.5
# Student Number: z5214836
# Student Name: Muhammed Tariq Mosaval

# imports -----------------------------
from socket import *
import sys
from datetime import datetime
import time
import os
import os.path
import threading
import collections
from collections import namedtuple
import random
import pickle
#---------------------------------------

# magic numbers
PORT_NUM = 50000
FILE_PORT_NUM = 8000
RESPONDER = 0
LOWEST = False

# global
blacklist = list()
flush_counter = 0
# Creating a struct like data structure to handle the CL arguments
peer_struct = namedtuple("peer_struct", "peer_identity successor_one successor_two MSS drop_prob")
current_peer = peer_struct(int(sys.argv[1]), int(sys.argv[2]) + PORT_NUM, \
    int(sys.argv[3]) + PORT_NUM, int(sys.argv[4]), float(sys.argv[5]))

# Class for creating and formatting the ACKS and log file text
class Acknowledgement:
    def __init__(self, sequence_number):
        self.timestamp = round(time.time(),2)
        self.ack_number = sequence_number
        self.num_bytes = current_peer.MSS
        self.next_sequence = 0

    def print_ack(self):
        print('Time Transmitted: ', self.timestamp)
        print('ACK Number: ', self.ack_number)
        print('Number of bytes: ', self.num_bytes)

    def get_ack_number(self):
        return self.next_sequence
    
    def expected_sequence_number(self):
        return self.ack_number + self.num_bytes

    def print_header(self, variable):
        if variable == "time":
            return self.timestamp
        if variable == "sequence":
            return self.expected_sequence_number()
        if variable == "mss":
            return self.num_bytes
        if variable == "ack":
            return self.next_sequence

# Class for creating and formatting the packets and log file text
class Packet:
    def __init__(self, sequenceNumber, packet,ack, eof = False):
        self.timestamp = round(time.time(),2)
        self.sequenceNumber = sequenceNumber
        self.packet = packet
        self.eof = eof
        self.ack = ack

    def print_packet(self):
        print('Time Transmitted: ', self.timestamp)
        print('Sequence Number: ', self.sequenceNumber)
        print('Packet Data: ', self.packet)
        print('End of File: ', self.eof)

    def get_packet_data(self):
        return self.packet
    
    def last_packet(self):
        return self.eof

    def print_header(self, variable):
        if variable == "time":
            return self.timestamp
        if variable == "sequence":
            return self.sequenceNumber
        if variable == "mss":
            return len(self.packet)
        if variable == "ack":
            return self.ack
    
# Class for keeping track of a peers successors and predecessors
class peer_relationship():
    def __init__(self):
        self.successor_one = None
        self.successor_two = None
        self.predecessor_one = None
        self.predecessor_two = None

    def update_successor(self, successor_identity, pos):
        if pos == 1:
            self.successor_one = successor_identity
        else:
            self.successor_two = successor_identity

    def update_predecessor(self, predecessory_identity, pos):
        if pos == 1:
            self.predecessor_one = predecessory_identity
        else:
            self.predecessor_two = predecessory_identity
    def get_successor(self, pos):
        if pos == 1:
            return self.successor_one
        else:
            return self.successor_two

    def get_predecessor(self, pos):
        if pos == 1:
            return self.predecessor_one
        else:
            return self.predecessor_two

    def is_lowest(self):
        if current_peer.peer_identity < self.predecessor_one - PORT_NUM and current_peer.peer_identity < self.predecessor_two - PORT_NUM:
            return True
        else:
            return False

    def fix_predecessors(self):
        if self.predecessor_one == self.predecessor_two:
            self.predecessor_one = None
        # Second lowest peer
        if (current_peer.peer_identity + PORT_NUM) < max(self.predecessor_one, self.predecessor_two) and not LOWEST and (self.predecessor_one is not None and self.predecessor_two is not None):
            if self.predecessor_one > self.predecessor_two:
                temp = self.predecessor_one
                self.predecessor_one = self.predecessor_two
                self.predecessor_two = temp
        if self.predecessor_one < self.predecessor_two and (self.predecessor_one is not None and self.predecessor_two is not None):
            if (current_peer.peer_identity + PORT_NUM) > max(self.predecessor_one, self.predecessor_two):
                temp = self.predecessor_one
                self.predecessor_one = self.predecessor_two
                self.predecessor_two = temp
        if LOWEST and (self.predecessor_one is not None and self.predecessor_two is not None):
            if self.predecessor_one < self.predecessor_two:
                temp = self.predecessor_one
                self.predecessor_one = self.predecessor_two
                self.predecessor_two = temp

    def fix_successors(self):
        if (self.successor_one != 0 and self.successor_two != 0) and (self.successor_one is not None and self.successor_two is not None):
            if self.successor_one == self.successor_two:
                self.successor_one = None
                print("succ = succ")
            # Second last peer
            if (current_peer.peer_identity + PORT_NUM) > min(self.successor_one, self.successor_two) \
                and (current_peer.peer_identity + PORT_NUM) < max(self.successor_one, self.successor_two) \
                    and (self.successor_one is not None and self.successor_two is not None):
                if self.successor_one < self.successor_two:
                    temp = self.successor_one
                    self.successor_one = self.successor_two
                    self.successor_two = temp
            # For all other peers
            elif self.successor_one > self.successor_two and (self.successor_one is not None and self.successor_two is not None):
                temp = self.successor_one
                self.successor_one = self.successor_two
                self.successor_two = temp

# create a peer_relationship object that handles successors and predecessors
connected_peers = peer_relationship()
connected_peers.update_successor(current_peer.successor_one, 1)
connected_peers.update_successor(current_peer.successor_two, 2)
listening_port = current_peer.peer_identity + PORT_NUM

client_udp = socket(AF_INET, SOCK_DGRAM)
peer_address = ('localhost', current_peer.peer_identity + PORT_NUM)
client_udp.bind(peer_address)

def flush():
    client_udp.setblocking(False)
    while 1:
        try:
            data = client_udp.recv(1024)
        except Exception:
            break
    client_udp.setblocking(True)
    return

# Class for making simple messages from a peer
class ping_message():
    def __init__(self, peer_number):
        self.peer_number = peer_number
    def ping_request(self):
        return f"request {self.peer_number}"
    def ping_response(self):
        return f"response {self.peer_number}"
    def file_request(self):
        return f"file {self.peer_number}"
    def file_response(self):
        return f"transfer {self.peer_number}"
    def peer_quit(self):
        return f"quit {self.peer_number}"
    def new_peers(self, dest, succ_one, succ_two):
        # message takes form: new <first/second> <first successor> <second successor>
        if dest == 1:
            return f"new first {succ_one} {succ_two}"
        else:
            return f"new second {succ_one} {succ_two}"
    def update_predecessor(self, dest, pred_one, pred_two):
        # message takes form: new <first/second> <first predecessor> <second predecessor>
        if dest == 1:
            return f"update first {self.peer_number} {pred_one} {pred_two}"
        else:
            return f"update second {self.peer_number} {pred_one} {pred_two}"
    def dead_peer(self, killed_peer, required_peer):
        return f"dead {killed_peer} {required_peer}"
    def new_successors(self, new_successor):
        return f"replace {new_successor}"

# Class for listening and responding to file requests and quit commands via tcp
class listening_socket_tcp(threading.Thread):
    def __init__(self, listening_port, peer_identity, connected_peers, MSS):
        threading.Thread.__init__(self)
        self.listening_port = listening_port
        self.connected_peers = connected_peers
        self.peer_identity = peer_identity
        self.ping_message = ping_message(int(peer_identity))
        self.MSS = MSS

    def run(self):
        client_tcp = socket(AF_INET,SOCK_STREAM)
        peer_address = ('localhost', self.listening_port)
        client_tcp.bind(peer_address)
        client_tcp.listen(1)

        while True:

            if current_peer.peer_identity in blacklist:
                print(blacklist)
                os._exit(0)

            # TCP connection established
            conn_tcp, address = client_tcp.accept()
            request = conn_tcp.recv(4096)
            incoming_message = request.decode().split()

            if incoming_message[0] == "file":
                # if the file is contained with this peer, start the sending routine
                if file_responsibilities(self.peer_identity, self.connected_peers, request.decode()):
                    print(f"\nFile {incoming_message[2]} is stored here.")
                    print(f"A response message, destined for peer {incoming_message[1]}, has been sent.")

                    # send the file request response message only to the requester
                    response = self.ping_message.file_response() + ' ' + incoming_message[1] + ' ' + incoming_message[2]
                    self.file_request_response(response)

                    # start the sender thread in this class' method
                    print("Starting file transfer ...\n")
                    self.file_transfer(incoming_message[1], incoming_message[2])

                # the file isnt contained here, send message onward
                else:
                    print(f"\nFile {incoming_message[2]} is not stored here.")
                    print("File request message has been forwarded to my successor.\n")
                    self.request_file(request.decode())
            
            if incoming_message[0] == "transfer":
                print(f"\nReceived a response message from peer {incoming_message[1]}, which has the file {incoming_message[3]}.")
                print("Begin receiving file ...\n")
                global RESPONDER
                RESPONDER = int(incoming_message[1])

            if incoming_message[0] == "quit":
                print(f'\nPeer {incoming_message[1]} will depart from the network\n')

            if incoming_message[0] == "new":
                # send the first predecessor its new successors
                if incoming_message[1] == 'first':
                    print(f"\nmy first successor is now {int(incoming_message[2]) - PORT_NUM}")
                    print(f"my second successor is now {int(incoming_message[3]) - PORT_NUM}\n")
                    self.connected_peers.update_successor(int(incoming_message[2]), 1)
                    self.connected_peers.update_successor(int(incoming_message[3]), 2)
                # send the second predecessor its new second successor
                if incoming_message[1] == 'second':
                    #print(f"first successor: {incoming_message[2]} second: {incoming_message[3]}")
                    print(f"\nmy first successor is now {self.connected_peers.get_successor(1) - PORT_NUM}")
                    print(f"my second successor is now {int(incoming_message[3]) - PORT_NUM}\n")
                    self.connected_peers.update_successor(int(incoming_message[3]), 2)

            if incoming_message[0] == "update":
                if incoming_message[1] == 'first':
                    self.connected_peers.update_predecessor(int(incoming_message[3]), 1)
                    self.connected_peers.update_predecessor(int(incoming_message[4]), 2)
                if incoming_message[1] == 'second':
                    self.connected_peers.update_predecessor(int(incoming_message[3]), 2)
            
            if incoming_message[0] == "dead":
                print("\nSomeone died...\n")
                # if the killed peer is the second successor of the alive peer
                if incoming_message[2] == '2':
                    conn_tcp.sendall(self.ping_message.new_successors(self.connected_peers.get_successor(1)).encode())
                # if the killed peer is the first successor of the alive peer
                if incoming_message[2] == '1':
                    conn_tcp.sendall(self.ping_message.new_successors(self.connected_peers.get_successor(1)).encode())              
                
            conn_tcp.close()

    def request_file(self, message):
        client_tcp = socket(AF_INET,SOCK_STREAM)
        successor_port = self.connected_peers.get_successor(1)
        client_tcp.connect(('localhost', successor_port))
        client_tcp.send(message.encode())
        client_tcp.close()
    
    def file_request_response(self, message):
        client_tcp = socket(AF_INET,SOCK_STREAM)
        requestor_port = int(message.split()[2]) + PORT_NUM
        client_tcp.connect(('localhost', requestor_port))
        client_tcp.send(message.encode())
        client_tcp.close()
    
    def file_transfer(self, requestor, file_name):
        # start up the sender thread to send files
        sender = sending_socket('send', requestor, file_name)
        sender.start()

    def departure(self):
        current_port = current_peer.peer_identity + PORT_NUM
        predecessor_array = [self.connected_peers.get_predecessor(1), self.connected_peers.get_predecessor(2)]
        successor_array = [self.connected_peers.get_successor(1), self.connected_peers.get_successor(2)]
        global blacklist
        
        minimum_successor = min(successor_array)
        maximum_successor = max(successor_array)
        minimum_predecessor = min(predecessor_array)
        maximum_predecessor = max(predecessor_array)

        # send quit messages to all predecessors
        for i in range(1, 3):
            client_tcp = socket(AF_INET,SOCK_STREAM)
            client_tcp.connect(('localhost', self.connected_peers.get_predecessor(i)))
            client_tcp.send(self.ping_message.peer_quit().encode())
            client_tcp.close()

        # If the lowest peer quits
        if LOWEST:
            #print("lowest peer")
            client_tcp = socket(AF_INET,SOCK_STREAM)
            first_predecessor = max(predecessor_array)
            client_tcp.connect(('localhost', first_predecessor))
            client_tcp.send(self.ping_message.new_peers(1, minimum_successor, maximum_successor).encode())
            client_tcp.close()

            client_tcp = socket(AF_INET,SOCK_STREAM)
            second_predecessor = min(predecessor_array)
            client_tcp.connect(('localhost', second_predecessor))
            client_tcp.send(self.ping_message.new_peers(2, minimum_successor, minimum_successor).encode())
            client_tcp.close()

        # if the quitting peer is the second lowest peer
        elif current_port < max(predecessor_array) and not LOWEST:
            #print("second lowest")
            client_tcp = socket(AF_INET,SOCK_STREAM)
            first_predecessor = min(predecessor_array)
            client_tcp.connect(('localhost', first_predecessor))
            client_tcp.send(self.ping_message.new_peers(1, minimum_successor, maximum_successor).encode())
            client_tcp.close()

            client_tcp = socket(AF_INET,SOCK_STREAM)
            second_predecessor = max(predecessor_array)
            client_tcp.connect(('localhost', second_predecessor))
            client_tcp.send(self.ping_message.new_peers(2, minimum_successor, minimum_successor).encode())
            client_tcp.close()

        # if the quitting peer is the second highest peer
        elif current_port > min(successor_array) and current_port < max(successor_array):
            #print("second highest")
            client_tcp = socket(AF_INET,SOCK_STREAM)
            first_predecessor = max(predecessor_array)
            client_tcp.connect(('localhost', first_predecessor))
            client_tcp.send(self.ping_message.new_peers(1, maximum_successor, minimum_successor ).encode())
            client_tcp.close()

            client_tcp = socket(AF_INET,SOCK_STREAM)
            second_predecessor = min(predecessor_array)
            client_tcp.connect(('localhost', second_predecessor))
            client_tcp.send(self.ping_message.new_peers(2, minimum_successor, maximum_successor).encode())
            client_tcp.close()
        

        # if the quitting peer is the highest peer
        elif current_port > max(successor_array):
            #print("highest")
            client_tcp = socket(AF_INET,SOCK_STREAM)
            first_predecessor = max(predecessor_array)
            client_tcp.connect(('localhost', first_predecessor))
            client_tcp.send(self.ping_message.new_peers(1, minimum_successor, maximum_successor).encode())
            client_tcp.close()

            client_tcp = socket(AF_INET,SOCK_STREAM)
            second_predecessor = min(predecessor_array)
            client_tcp.connect(('localhost', second_predecessor))
            client_tcp.send(self.ping_message.new_peers(2, minimum_successor, minimum_successor).encode())
            client_tcp.close()

        # for all other peers that arent one of the above:
        else:
            #print("all else")
            client_tcp = socket(AF_INET,SOCK_STREAM)
            first_predecessor = max(predecessor_array)
            client_tcp.connect(('localhost', first_predecessor))
            client_tcp.send(self.ping_message.new_peers(1, minimum_successor, maximum_successor).encode())
            client_tcp.close()

            client_tcp = socket(AF_INET,SOCK_STREAM)
            second_predecessor = min(predecessor_array)
            client_tcp.connect(('localhost', second_predecessor))
            client_tcp.send(self.ping_message.new_peers(2, minimum_successor, minimum_successor).encode())
            client_tcp.close()
    
        # cancel the predecessors of the quitting peers' successors
        client_tcp = socket(AF_INET,SOCK_STREAM)
        client_tcp.connect(('localhost', minimum_successor))
        client_tcp.send(self.ping_message.update_predecessor(1, minimum_predecessor, maximum_predecessor).encode())
        client_tcp.close()

        client_tcp = socket(AF_INET,SOCK_STREAM)
        client_tcp.connect(('localhost', maximum_successor))
        client_tcp.send(self.ping_message.update_predecessor(2, maximum_predecessor, maximum_predecessor).encode())
        client_tcp.close()

        global connected_peers
        for i in range(1,3):
            connected_peers.update_successor(0, i)
            connected_peers.update_predecessor(0, i)
        blacklist.append(current_peer.peer_identity)

    def peer_killed(self, dead_peer, alive_peer, required_peer):
        client_tcp = socket(AF_INET,SOCK_STREAM)
        client_tcp.connect(('localhost', alive_peer))
        client_tcp.send(self.ping_message.dead_peer(dead_peer, required_peer).encode())
        data = client_tcp.recv(1024).decode().split()
        if data[0] == "replace":
            succ_array = [self.connected_peers.get_successor(1), self.connected_peers.get_successor(2)]
            if succ_array[0] is None:
                print(f"\nMy first peer is now {self.connected_peers.get_successor(2) - PORT_NUM}")
                print(f"My second peer is now {int(data[1]) - PORT_NUM}\n")
                self.connected_peers.update_successor(self.connected_peers.get_successor(2), 1)
                self.connected_peers.update_successor(int(data[1]), 2)
            if succ_array[1] is None:
                print(f"\nMy first peer is now {self.connected_peers.get_successor(1) - PORT_NUM}")
                print(f"My second peer is now {int(data[1]) - PORT_NUM}\n")
                self.connected_peers.update_successor(int(data[1]), 2)

        #self.connected_peers.fix_successors()
        client_tcp.close()

# Class for listening to and responding to pings via udp
class listening_socket_udp(threading.Thread):
    def __init__(self, listening_port, peer_identity, connected_peers):
        threading.Thread.__init__(self)
        self.listening_port = listening_port
        self.connected_peers = connected_peers
        self.ping_message = ping_message(int(peer_identity))
        self.file_packet_list = list()
        
    def run(self):
        # UDP socket for messages
        #t = threading.Thread(target=self.flush()).start()
        deck_request = collections.deque()
        deck_response = collections.deque()
        pred_array = [self.connected_peers.get_predecessor(1),self.connected_peers.get_predecessor(2)]
        global blacklist
        global client_udp
        while True:
            # UDP connection established
            if current_peer.peer_identity in blacklist:
                print(blacklist)
                os._exit(0)
            self.send_pings()
            data_udp, address = client_udp.recvfrom(1024)
            if data_udp:
                # try-catch for string messages
                try:
                    stringdata = data_udp.decode().split()
                    predecessor_one = self.connected_peers.get_predecessor(1)
                    predecessor_two = self.connected_peers.get_predecessor(2)
                    successor_one = self.connected_peers.get_successor(1)
                    successor_two = self.connected_peers.get_successor(2)

                    # code for incoming request messages
                    if stringdata[0] == "request":

                        # code to see if the peers predecessors are still there
                        deck_request.append(int(stringdata[1]))
                        if len(deck_request) == 5:
                            retain_one = False
                            retain_two = False
                            while True:
                                try:
                                    val = deck_request.pop() + PORT_NUM
                                    if val == predecessor_one:
                                        retain_one = True
                                    if val == predecessor_two:
                                        retain_two = True
                                except IndexError:
                                    break
                            if not retain_one:
                                self.connected_peers.update_predecessor(None, 1)
                            if not retain_two:
                                self.connected_peers.update_predecessor(None, 2)
                        
                        print(f"A ping request message was received from Peer {stringdata[1]}")
                        if predecessor_one is None and (int(stringdata[1]) + PORT_NUM) not in blacklist \
                            and (int(stringdata[1]) + PORT_NUM) != predecessor_two:
                            self.connected_peers.update_predecessor(int(stringdata[1])+PORT_NUM,1)
                        elif predecessor_two is None and (int(stringdata[1]) + PORT_NUM) not in blacklist \
                            and (int(stringdata[1]) + PORT_NUM) != predecessor_one:
                            self.connected_peers.update_predecessor(int(stringdata[1])+PORT_NUM,2)
                        if (int(stringdata[1]) + PORT_NUM) not in blacklist:
                            client_udp.sendto(self.ping_message.ping_response().encode("utf-8"), ('localhost', PORT_NUM + int(stringdata[1])))
                        global LOWEST
                        LOWEST = self.connected_peers.is_lowest()
                        self.connected_peers.fix_predecessors()
                    
                    # code for responding to a response message
                    if stringdata[0] == "response":
                        # code to see if the peers successors are still there
                        deck_response.append(int(stringdata[1]))
                        if len(deck_response) == 7:
                            retain_one = False
                            retain_two = False
                            print("Checking peer connections...")
                            while True:
                                try:
                                    val = deck_response.pop() + PORT_NUM
                                    if val == successor_one:
                                        retain_one = True
                                    if val == successor_two:
                                        retain_two = True
                                except IndexError:
                                    break
                            if not retain_one:
                                temp = successor_one
                                if successor_one:
                                    print(f"\nPeer {successor_one - PORT_NUM} is no longer alive.\n")
                                self.connected_peers.update_successor(None, 1)
                                listener_tcp.peer_killed(temp, successor_two, 1)
                            if not retain_two:
                                temp = successor_two
                                if successor_two:
                                    print(f"\nPeer {successor_two - PORT_NUM} is no longer alive.\n")
                                self.connected_peers.update_successor(None, 2)
                                listener_tcp.peer_killed(temp, successor_one, 2)
                            
                        if successor_one is None and (int(stringdata[1]) + PORT_NUM) not in blacklist \
                            and int(stringdata[1] + PORT_NUM) != successor_two:
                            self.connected_peers.update_successor(int(stringdata[1])+PORT_NUM,1)
                        elif successor_two is None and (int(stringdata[1]) + PORT_NUM) not in blacklist \
                            and int(stringdata[1] + PORT_NUM) != successor_one:
                            self.connected_peers.update_successor(int(stringdata[1])+PORT_NUM,2)
                        print(f"A ping response message was received from Peer {stringdata[1]}")
                        #self.connected_peers.fix_successors()
                except Exception:
                    pass
                
    def send_pings(self):
        global client_udp
        global flush_counter
        if flush_counter == 10:
            flush()
            flush_counter = 0
        
        if current_peer.peer_identity in blacklist:
            os._exit(0)
        if current_peer.peer_identity not in blacklist:
            if self.connected_peers.get_successor(1):
                client_udp.sendto(self.ping_message.ping_request().encode("utf-8"), ('localhost', self.connected_peers.get_successor(1)))
            if self.connected_peers.get_successor(2):
                client_udp.sendto(self.ping_message.ping_request().encode("utf-8"), ('localhost', self.connected_peers.get_successor(2)))
        flush_counter += 1
        time.sleep(1)

    
# Class for sending and receiving files via udp
class sending_socket(threading.Thread):
    def __init__(self, sender_type, requestor = 0, file_name = ''):
        threading.Thread.__init__(self)
        self.peer = current_peer.peer_identity
        self.MSS = current_peer.MSS
        self.drop_p = current_peer.drop_prob
        self.requestor = requestor
        self.file_name = file_name
        self.packet_list = list()
        self.received_packet_list = list()
        self.sender_type = sender_type

    # Divides the file into chunks the size of the MSS and stores them along with
    # their sequence number into packet_list. They are stored as Packet objects
    def divide_file(self):
        sequence_number = 1
        try:
            with open(self.file_name + '.pdf', "rb") as f:
                data = f.read()
                i = 0
                length = sys.getsizeof(data)
                while i <= length:
                    f.seek(i)
                    chunk = f.read(self.MSS)
                    if i + self.MSS > length:
                        self.packet_list.append(Packet(sequence_number,chunk, 0, True))
                    else:
                        self.packet_list.append(Packet(sequence_number,chunk, 0, False))
                    i += self.MSS
                    sequence_number = sequence_number + self.MSS
        except FileNotFoundError:
            print(f"No file {self.file_name} found.")

    # Start the sender
    def run(self):
        try:
            file_udp = socket(AF_INET, SOCK_DGRAM)
            peer_address = ('localhost', current_peer.peer_identity + FILE_PORT_NUM)
            file_udp.bind(peer_address)
        except Exception:
            pass

        if self.sender_type == 'send':
            self.send_file(file_udp)
        else:
            self.receive_file(file_udp)
    
    def send_file(self, file_udp):

        responding_log = open('responding_log.txt', 'w')

        self.divide_file()
        for i in range(0, len(self.packet_list)):
            current_packet = self.packet_list[i]
            pickled_packet = pickle.dumps(current_packet)
            # if a random number is greater than the drop probability, send the packet
            rng = random.random()
            RTX = 0
            while rng < self.drop_p:
                #print(f"packet {current_packet.print_header('sequence')} dropped")
                if RTX == 0:
                    responding_log.write(f"Drop\t{current_packet.print_header('time')}\t{current_packet.print_header('sequence')}\t{current_packet.print_header('mss')}\t{current_packet.print_header('ack')}\n")
                else:
                    responding_log.write(f"RTX/Drop\t{current_packet.print_header('time')}\t{current_packet.print_header('sequence')}\t{current_packet.print_header('mss')}\t{current_packet.print_header('ack')}\n")
                RTX += 1
                rng = random.random()

            while True:
                #print(f'sent sequence number: {current_packet.print_header("sequence")}, ACK: {current_packet.print_header("ack")}')
                if RTX > 0:
                    responding_log.write(f"RTX\t{current_packet.print_header('time')}\t{current_packet.print_header('sequence')}\t{current_packet.print_header('mss')}\t{current_packet.print_header('ack')}\n")
                else:
                    responding_log.write(f"snd\t{current_packet.print_header('time')}\t{current_packet.print_header('sequence')}\t{current_packet.print_header('mss')}\t{current_packet.print_header('ack')}\n")
                file_udp.sendto(pickled_packet, ('localhost', int(self.requestor) + FILE_PORT_NUM))
                file_udp.settimeout(1)
                # if the requester has responded in the last second...
                try:
                    data, address = file_udp.recvfrom(4096)
                    received_ACK = pickle.loads(data)
                    next_ACK = received_ACK.expected_sequence_number()
                    #print(f"received ACK: {next_ACK}")
                    if current_packet.last_packet():
                        break
                    if next_ACK == self.packet_list[i+1].print_header('sequence'):
                        responding_log.write(f"rcv\t{received_ACK.print_header('time')}\t{received_ACK.print_header('ack')}\t{received_ACK.print_header('mss')}\t{received_ACK.print_header('sequence')}\n")
                        rng = 0
                        break
                except timeout:
                    print("requester timed out")
        print("\nThe file is sent.\n")
        responding_log.close()

    def receive_file(self, file_udp):

        requesting_log = open('requesting_log.txt', 'w')
        while True:
            data_udp, address = file_udp.recvfrom(4096)
            if data_udp:
                pick = pickle.loads(data_udp)
                if isinstance(pick, Packet):
                    requesting_log.write(f"rcv\t{pick.print_header('time')}\t{pick.print_header('sequence')}\t{pick.print_header('mss')}\t{pick.print_header('ack')}\n")
                    ACK = Acknowledgement(pick.print_header('sequence'))
                    #print(f"received seq: {pick.print_header('sequence')}, bytes: {pick.print_header('mss')}, ACK: {pick.print_header('ack')}")
                    #print(f"sent ACK: {ACK.get_ack_number()}, expected sequence: {ACK.expected_sequence_number()}")
                    file_udp.sendto(pickle.dumps(ACK), ('localhost', RESPONDER + FILE_PORT_NUM))
                    requesting_log.write(f"snd\t{ACK.print_header('time')}\t{ACK.print_header('ack')}\t{ACK.print_header('mss')}\t{ACK.print_header('sequence')}\n")
                    self.received_packet_list.append(data_udp)
                    if pick.last_packet():
                        print("\nThe file is received.\n")
                        break

        requesting_log.close()
        with open('received_file.pdf', 'wb') as f:
            for i in self.received_packet_list:
                unpickled_packet = pickle.loads(i)
                binary = unpickled_packet.get_packet_data()
                f.write(binary)

# create the listening port thread for tcp messages
listener_tcp = listening_socket_tcp(listening_port, current_peer.peer_identity, connected_peers, current_peer.MSS)
listener_tcp.start()

# create the listening port thread for udp messages
listener_udp = listening_socket_udp(listening_port, current_peer.peer_identity, connected_peers)
listener_udp.start()


def main():
    # create a ping_message object that makes messages
    peer_message = ping_message(current_peer.peer_identity)

    # await user input for "request [file]" or "quit"
    
    user_input = input()
    while user_input != "quit":
        if user_input.split()[0] == "request" or user_input.split()[0] == "Request":
            print(f"\nFile request message for {user_input.split()[1]} has been sent to my successor.\n")

            # Create a request message from this peer and send it through the DHT
            request_message = peer_message.file_request() + ' ' + user_input.split()[1]
            listener_tcp.request_file(request_message)

            # Start this peers own Sender thread
            sender = sending_socket('receive')
            sender.start()

        elif user_input.split()[0] == "pred":
            print(f"\npredecessor 1: {connected_peers.get_predecessor(1)} and 2: {connected_peers.get_predecessor(2)}\n")
        elif user_input.split()[0] == "succ":
            print(f"\nsuccessor 1: {connected_peers.get_successor(1)} and 2: {connected_peers.get_successor(2)}\n")
        user_input = input()

    
    listener_tcp.departure()
    os._exit(0)

def file_hash_value(file_name):
    return int(file_name) % 256

# Function that determines if the current peer is responsible for the requested file
def file_responsibilities(peer_identity, connected_peers, message):
    cur_dir = os.getcwd()
    file_list = os.listdir(cur_dir)         # returns a list of all the files in the cur_dir
    parent_dir = os.path.dirname(cur_dir)   # string of the cur_dir path
    
    peer_number = int(peer_identity)
    pred_one = connected_peers.get_predecessor(1) - PORT_NUM
    pred_two = connected_peers.get_predecessor(2) - PORT_NUM

    min_array = [peer_number]
    if message.split()[2] == '0000':
        target_file = 0
    else:
        target_file = int(message.split()[2].lstrip('0'))
    file_hash = file_hash_value(target_file)
    # iterate through all the files in the current directory. If it is a pdf, check if
    # it is valid and if its the requested file. Then check if the current peer is
    # responsible for it.

    for file_name in file_list:
        extension = os.path.splitext(file_name)[1]
        if extension == ".pdf":
            if file_hash == 0 or check_if_int(file_name.split('.')[0].lstrip('0')) and int(file_name.split('.')[0].lstrip('0')) == target_file:
                # if the hash is the same as the peer
                if file_hash == peer_number:
                    return True
                # if the hash is greater than the lowest peer
                if LOWEST == True and file_hash > peer_number and file_hash > pred_one and file_hash > pred_two:
                    return True
                # if the hash is greater than a non-lowest peer
                if file_hash > peer_number:
                    return False
                # if the hash is less than a peer
                if file_hash < peer_number:
                    if pred_one > file_hash:
                        min_array.append(pred_one)
                    if pred_two > file_hash:
                        min_array.append(pred_two)
                    nearest = min(min_array, key=lambda v: abs(file_hash - v))
                    if nearest == peer_number:
                        return True
                else:
                    return False

def check_if_int(file_name):
    try: 
        int(file_name)
        return True
    except ValueError:
        return False 

try:
    main()
except KeyboardInterrupt:
    print('\nUngracefully Exited')
    os._exit(0)
        



