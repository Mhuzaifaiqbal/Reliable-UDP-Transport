import time
from queue import Queue,Empty
from typing import Tuple
from socket import socket
import util
import random
Address = Tuple[str, int]




class MessageSender:
    '''
    DO NOT EDIT ANYTHING IN THIS CLASS
    '''

    def __init__(self, sock: socket, receiver_addr: Address, msg_id: int):
        self.__sock: socket = sock
        self.__receiver_addr = receiver_addr
        self.__msg_id = msg_id

    def send(self, packet: str):
        ''' wraps our msg in a metadata'''
        self.__sock.sendto(
            (f"s:{str(self.__msg_id)}:{packet}").encode("utf-8"),
            self.__receiver_addr)


class ReliableMessageSender(MessageSender):
    '''
    This class reliably delivers a message to a receiver.
    You have to implement the send_message and on_packet_received methods.
    You can use self.send(packet) to send a packet to the receiver.
    You can add as many helper functions as you want.
    '''




    def __init__(self, sock: socket, receiver_addr: Address, msg_id: int,
                 window_size: int):
        MessageSender.__init__(self, sock, receiver_addr, msg_id)
        '''
        This is the constructor of the class where you can define any class attributes.
        window_size is the size of your message transport window
        (the number of in-flight packets during message transmission).
        Ignore other arguments; they are passed to the parent class.
        You should immediately return from this function and not block.
        '''
        self.window_size=window_size
        #we have a message sender function already created

        #we need to first create an array so we can basically store packets of
        #a specific message
        self.msgPackets=[]  #when we break down the long message into multiple chunks and
        # then assign the
        #sequence number we will use this to track our packets
        #it is also important to track the time of when the messages are sent
        self.timer={}

        #now for acks, it is important for reliable transport
        # so we need to track them starting from a number till the end
        self.oldest=0#this will be our start packet
        self.next_pkt=0 #this will be the next in line packet

        #our client will acknowledge all packets it receives so for that
        self.ack_received=set()
        self.queue=Queue()


    def reliable_sending(self,packet,seq):
        self.send(packet)
        self.timer[seq] = time.time()

        while True:
            try:
                #now we wait for a specific time for the a ack
                #agar wo hogya to simply return warna phir se bhejna aur wait karna
                ack = self.queue.get(timeout=util.TIME_OUT)
                if ack == seq + 1:
                    return True
            except Empty :
                self.send(packet)
                self.timer[seq] = time.time()

    def process_acks(self, start_seq, end_seq):
        """Process all ACKs in the queue and slide window"""
        #basically for all acks that are received, we have to check and mark the packets
        #and then using this we will slide the window over marked packets
        #and sice for acks we are using a queue, it makes it a lot easier
        while True:
            try:
                ack = self.queue.get_nowait()
                # Cumulative ACK: ack means all packets before ack are received
                for seq in range(start_seq + 1, min(ack, end_seq)):
                    self.ack_received.add(seq)

                while self.oldest in self.ack_received:
                    self.oldest += 1

            except Empty:
                break
    def sliding_window(self, start_seq, end_seq):
        '''making this helper msg for window sliding'''
        # we have the oldest packet or the first packet being sent in the window
        #following the next packet
        self.oldest = start_seq + 1
        self.next_pkt = start_seq + 1
        #since a completely new ack is being set for the window
        #clear the set
        self.ack_received.clear()

        #we have 3 main things to do
        #1. send packets
        #2. Handle timeouts
        #3. retransmission in case of timoouts

        #creating different helper functions because it is easier
        while self.oldest<end_seq:
            self.window_send(start_seq,end_seq) #one done
            #we need to call process agaiun and again so that the window keeps
            # sliding correctly as the
            #quueue fills up
            self.process_acks(start_seq, end_seq)

            self.handle_timeouts(start_seq)

            time.sleep(0.01)



    #I will jsut create helper fucntions to make it eaiser for me to understand the code

    def window_send(self, start_seq, end_seq):
        """
    Sends packets in the current window of the sliding window protocol."""
        while self.next_pkt < self.oldest + self.window_size and self.next_pkt<end_seq:

            idx = self.next_pkt - (start_seq + 1)
            if idx<len(self.msgPackets):
                packet = self.msgPackets[idx]
                self.send(packet)
                self.timer[self.next_pkt] = time.time()
                self.next_pkt += 1

    def handle_timeouts(self, start_seq):
        """Checks for packet timeouts and triggers retransmission if needed."""
        current_time = time.time()
        for seq in range(self.oldest, self.next_pkt):

            if seq in self.ack_received:
                continue
            if current_time - self.timer.get(seq, 0) > util.TIME_OUT:
                #if we have waited enough and still havent received our ack then
                #we resend
                for resen_seq in range(self.oldest, self.next_pkt):

                    if resen_seq in self.ack_received:
                        continue

                    idx = resen_seq - (start_seq + 1)

                    self.send(self.msgPackets[idx])
                    self.timer[resen_seq] = current_time
                break





    def on_packet_received(self, packet: str):
        '''
        TO BE IMPLEMENTED BY STUDENTS

        This method is invoked whenever a packet is received from the receiver.
        Ideally, only ACK packets should be received here.
        You would have to use a way to communicate these packets to the send_message method.
        One way is to use a queue: you can enqueue packets to it in this method, and dequeue
        them in send_message.
        You can also use the timeout argument of a queue’s dequeue method to implement timeouts
        in this assignment.
        You should immediately return from this method and not block.
        '''

        if not util.validate_checksum(packet):
            return

        #we have a helper function to parse the packets
        pkt_type, seq,data,checksum = util.parse_packet(packet)
        # i have already created a queue
        if pkt_type == "ack":
            self.queue.put(int(seq))

    def send_message(self, message: str):

        ''''
        TO BE IMPLEMENTED BY STUDENTS

        This method reliably sends the passed message to the receiver.
        This method does not need to spawn a new thread and return immediately;
        it can block indefinitely until the message is completely received by the receiver.
        You can send a packet to the receiver by calling self.send(...).

        Sender's logic:
        1) Break down the message into util.CHUNK_SIZE sized chunks.
        2) Choose a random sequence number to start the communication from.
        3) Reliably send a start packet. (i.e. wait for its ACK and resend the packet if
        the ACK is not received within util.TIME_OUT seconds.)
        4) Send out a window of data packets and wait for ACKs to slide the window appropriately.
        5) How to slide the window? Suppose that the current window starts at sequence number
        j. If you receive an ACK of sequence number k, such that k > j, send the subsequent
        k – j number of chunks. Note that the window now starts from sequence number j + (k – j).
        6) If you receive no ACKs for util.TIME_OUT seconds, resend all the packets in the
        current window.
        7) Once all the chunks have been reliably sent, reliably send an end packet.
        '''
        self.msgPackets.clear()
        self.timer.clear()
        self.ack_received.clear()
        #breaking into chunks
        chunks=[]
        i=0
        while i<len(message):
            chunks.append(message[i:i+util.CHUNK_SIZE])
            i+=util.CHUNK_SIZE

        #now we need to set random seq numbers
        #okay so i was thinking why not keep it simple in case of sequence numbers
        #like start from 0 and go up in order
        #but that would cause confusion with older messages and make it easy for hackers to mess
        # the system
        # so random is the solution
        #the problem that may arise is if the get the same number, but for that we can use a very
        # big range so in that case
        #the probability of getting the same number is very low
        starting_Seq=random.randint(2000,80000)
        # first_packet=util.make_message("start",starting_Seq)
        # i used the wrong function
        first_packet=util.make_packet("start",starting_Seq)

        #now that we have the first packet, we need to make packets for each chunk with a
        # unique
        #sequence number so we can send them in order withouth confusion
        for i in range(len(chunks)):
            seq = starting_Seq + 1 + i
            packet = util.make_packet("data", seq, chunks[i])
            self.msgPackets.append(packet)
        #when this is finally done i will simply create an end packet to mark the end of the
        # msg
        ending_seq = starting_Seq + 1 + len(chunks)
        end_packet = util.make_packet("end", ending_seq)

        self.reliable_sending(first_packet, starting_Seq)

        if chunks:
            self.sliding_window(starting_Seq, ending_seq)

        self.reliable_sending(end_packet, ending_seq)

        # raise NotImplementedError


#acha some imp things that i havbe to keep in mind
#packets out of order aa saktay
#duplicates aa saktay
#acks manage karnay step by step
#so we have to store the packets somewhere and basically parse them by the helper provided'
#process after we get start and then collect msgs in the correct order


class MessageReceiver:
    '''
    DO NOT EDIT ANYTHING IN THIS CLASS
    '''

    def __init__(self, sock: socket, sender_addr: Address, msg_id: int,
                 completed_message_q: Queue):
        self.__sock: socket = sock
        self.__sender_addr = sender_addr
        self.__msg_id = msg_id
        self.__completed_message_q = completed_message_q

    def send(self, packet: str):
        self.__sock.sendto(
            (f"r:{str(self.__msg_id)}:{packet}").encode("utf-8"),
            self.__sender_addr)

    def on_message_completed(self, message: str):
        self.__completed_message_q.put(message)


class ReliableMessageReceiver(MessageReceiver):
    '''
    This class reliably receives a message from a sender.
    You have to implement the on_packet_received method.
    You can use self.send(packet) to send a packet back to the sender, and will have to call
    self.on_message_completed(message) when the complete message is received.
    You can add as many helper functions as you want.
    '''

    def __init__(self, sock: socket, sender_addr: Address, msg_id: int,
                 completed_message_q: Queue):
        MessageReceiver.__init__(self, sock, sender_addr, msg_id,
                                 completed_message_q)
        '''
        This is the constructor of the class where you can define any class attributes
        to maintain state.
        You should immediately return from this function and not block.
        '''
        self.buffer={} #to store packets
        self.chunks=[] # to allow ordered chunks
        # so we also have a sequence number and expected sequence number
        #like if receiver got 10, it will increment the expected sequence number]
        #to 11 because of ordering. if say sender sends 12, it will know that the wrong
        #one is sent.
        self.expected_seq=None
        self.rec_start=False

    def on_packet_received(self, packet: str):
        '''
        TO BE IMPLEMENTED BY STUDENTS

        This method is invoked whenever a packet is received from the sender.
        You have to inspect the packet and determine what to do.
        You should immediately return from this method and not block.
        You can either ignore the packet, or send a corresponding ACK packet back to
         the sender by calling self.send(packet).
        If you determine that the sender has completely sent the message,
        call self.on_message_completed(message) with the completed message as its argument.

        Receiver’s logic:
        1) When you receive a packet, validate its checksum and ignore it if it is corrupted.
        2) Inspect the packet_type and sequence number.
        3) If the packet type is "start", prepare to store incoming chunks of data in
          some data structure and send an ACK back to the sender with the received packet’s
          sequence number + 1.
        4) If the packet type is "data", store it in an appropriate data type
        (if it is not a duplicate packet you already have stored), and send a corresponding
        cumulative ACK. (ACK with the sequence number for which all previous packets have
        been received).
        5) If the packet type is "end", assemble all the stored chunks into a message,
        call self.on_message_received(message) with the completed message, and send an
        ACK with the received packet’s sequence number + 1.
        '''
        # raise NotImplementedError
        #we have to validate the checksum first
        if not util.validate_checksum(packet):
            return
        #we are also given a function to parse the packet, it will
        #tell us the type of packet

        # def parse_packet(packet):
        #     '''
        #     This function will parse the packet in the same way it was made in the
        # above function.
        #     '''
        #     pieces = packet.split('|')
        #     pck_type, seqno = pieces[0:2]
        #     checksum = pieces[-1]
        #     data = '|'.join(pieces[2:-1])
        #     return pck_type, seqno, data, checksum
        pkt_type,seqno_str,data,checksum=util.parse_packet(packet)

        #seq number returned is a string so i need to convert to an int first
        seqno=int(seqno_str)

        #now the question has 3 different scenarios
        #the first one is that if thepacket is start then we have to store the chucks and send
        # ackks back with the expected sequence number
        if pkt_type=="start":
            self.expected_seq=seqno+1
            self.buffer.clear()
            self.chunks.clear()
            self.rec_start=True

            #now that we have received the start pakcket, we simply need to send back the ack
            #and also cleart all( the buffers and chunks because a new msg has been started
            ack=util.make_packet("ack",self.expected_seq)
            self.send(ack)
            return

            #the second condition is that of end

        elif pkt_type=="end":
            #now what if the start packet was never received? it means ]
            # that it was a bug which can cause incomplete
            # or invalidb msgs and this should be handled
            if not self.rec_start:
                return
            #now we have to see if everything was reveived uptil the enf packert
            if self.expected_seq==seqno:
                message = ''.join(self.chunks)
                self.on_message_completed(message)

            ack = util.make_packet("ack", seqno + 1)
            self.send(ack)
        elif pkt_type=="data":
            # for this i will create a simple helper function
            self.data_case(seqno,data)

    def data_case(self, seq, data):
        if not self.rec_start:
            return
        #in case we receive all the expected or correct packets
        #we just add it in chunks
        if seq == self.expected_seq:
            self.chunks.append(data)
            self.expected_seq += 1
            while self.expected_seq in self.buffer:
                self.chunks.append(self.buffer.pop(self.expected_seq))
                self.expected_seq += 1
                #but we also need to handle out of order transmissions
        elif seq > self.expected_seq:
            # Store it for later, but only if new
            if seq not in self.buffer:
                self.buffer[seq] = data

        ack_packet = util.make_packet("ack", self.expected_seq)
        self.send(ack_packet)
