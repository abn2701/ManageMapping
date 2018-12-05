using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NetMQ;
using NetMQ.Sockets;
using System.Collections.Concurrent;
using log4net;
using System.Threading;
using System.Runtime.Serialization.Formatters.Binary;
using System.IO;

namespace OmsnfManageMapping
{
    class ZmqSocket
    {
        private NetMQSocket socket;
        internal ConcurrentDictionary<string, NetMQSocket> socketDictionary = new ConcurrentDictionary<string, NetMQSocket>();
        internal ConcurrentDictionary<string, Queue<byte[][]>> socketQueue = new ConcurrentDictionary<string, Queue<byte[][]>>();
        private ILog _errlog = LogManager.GetLogger("Error");
        public bool Start(ZmqSocketType ZMQType, bool Flag, string topic, string[] topic_tcp)
        {
            try
            {
                switch (ZMQType)
                {
                    case ZmqSocketType.Pub: socket = new PublisherSocket(); break;
                    case ZmqSocketType.Sub: socket = new SubscriberSocket(); break;
                }
                socket.Options.SendHighWatermark = 10000;
                socket.Options.ReceiveHighWatermark = 10000;
                socket.Options.TcpKeepalive = true;
                socket.Options.TcpKeepaliveIdle = TimeSpan.FromMinutes(1);
                socket.Options.TcpKeepaliveInterval = TimeSpan.FromSeconds(10);

                if (Flag)
                {
                    foreach (string tcp in topic_tcp)
                    {
                        try
                        {
                            if (topic == "OmsManageMapping.ZfPub.MsgMapping.ZfSub.OmsManageSystem" && socketDictionary.ContainsKey("OmsManageMapping.ZfPub.MsgInitMapping.ZfSub.OmsManageSystem"))
                            {
                                socket = socketDictionary["OmsManageMapping.ZfPub.MsgInitMapping.ZfSub.OmsManageSystem"];
                            }
                            else if (topic == "OmsManageMapping.ZfPub.MsgInitMapping.ZfSub.OmsManageSystem" && socketDictionary.ContainsKey("OmsManageMapping.ZfPub.MsgMapping.ZfSub.OmsManageSystem"))
                            {
                                socket = socketDictionary["OmsManageMapping.ZfPub.MsgMapping.ZfSub.OmsManageSystem"];
                            }
                            else if (topic == "OmsManageSystem.ZfPub.MsgRequest.ZfSub.OmsManageMapping" && socketDictionary.ContainsKey("OmsManageSystem.ZfPub.MsgMapping.ZfSub.OmsManageMapping"))
                            {
                                socket = socketDictionary["OmsManageSystem.ZfPub.MsgMapping.ZfSub.OmsManageMapping"];
                                AddQueue(topic);
                            }
                            else if (topic == "OmsManageSystem.ZfPub.MsgMapping.ZfSub.OmsManageMapping" && socketDictionary.ContainsKey("OmsManageSystem.ZfPub.MsgRequest.ZfSub.OmsManageMapping"))
                            {
                                socket = socketDictionary["OmsManageSystem.ZfPub.MsgRequest.ZfSub.OmsManageMapping"];
                                AddQueue(topic);
                                (socket as SubscriberSocket).Subscribe(topic);
                            }
                            else
                            {
                                Console.WriteLine($"Topic:{topic}");
                                socket.Bind(tcp);
                                Console.WriteLine($"socket Bind:{tcp}");
                                switch (ZMQType)
                                {
                                    case ZmqSocketType.Sub:
                                        Console.WriteLine($"AddQueue1:{topic}");
                                        AddQueue(topic);
                                        (socket as SubscriberSocket).Subscribe(topic);
                                        Task.Run(() =>
                                        {
                                            ReceiveData(socket, topic);
                                        });
                                        break;
                                }
                            }
                        }
                        catch
                        {
                            Console.WriteLine($"socket Bind error:{tcp}");
                        }
                    }
                }
                else
                {
                    foreach (string tcp in topic_tcp)
                    {
                        try
                        {
                            socket.Connect(tcp);
                        }
                        catch
                        { }
                    }

                    switch (ZMQType)
                    {
                        case ZmqSocketType.Sub:
                            AddQueue(topic);
                            (socket as SubscriberSocket).Subscribe(topic);
                            Task.Run(() =>
                            {
                                ReceiveData(socket, topic);
                            });
                            break;
                    }
                }
                //switch (ZMQType)
                //{
                //    case ZmqSocketType.Sub:
                //        AddQueue(topic);
                //        (socket as SubscriberSocket).Subscribe(topic);
                //        Task.Run(() =>
                //        {
                //            ReceiveData(socket, topic);
                //        });
                //        break;
                //}
                socketDictionary.TryAdd(topic, socket);
                return true;
            }
            catch (Exception ex)
            {
                _errlog.Error($"DotNetZmq Start: {ex.Message}\r\n{ex.StackTrace}\r\n{ex.Source}");
                return false;
            }
        }
        private void ReceiveData(NetMQSocket socket, string topic)
        {
            try
            {
                //Queue<byte[][]> _Q = new Queue<byte[][]>();
                //if (!socketQueue.Keys.Contains(topic))
                //{
                //    socketQueue.TryAdd(topic, _Q);
                //}
                while (true)
                {
                    try
                    {
                        byte[][] item = new byte[4][];
                        item[0] = socket.ReceiveFrameBytes();
                        Console.WriteLine("Receive: " + Encoding.UTF8.GetString(item[0]));
                        int i = 0;
                        while (socket.Options.ReceiveMore)
                        {
                            i++;
                            socket.TryReceiveFrameBytes(out item[i]);
                            Console.WriteLine($"Receive{i}: " + Encoding.UTF8.GetString(item[i]));
                        }
                        //_Q.Enqueue(item);
                        socketQueue[topic].Enqueue(item);
                    }
                    catch (Exception ex)
                    {
                        _errlog.Error($"Zmq ReceiveData: {ex.Message}\r\n{ex.StackTrace}\r\n{ex.Source}");
                    }
                    Thread.Sleep(1);
                }
            }
            catch (Exception ex)
            {
                _errlog.Error($"Zmq ReceiveData: {ex.Message}\r\n{ex.StackTrace}\r\n{ex.Source}");
            }
        }
        internal void SendData(NetMQSocket socket, string topic, byte[][] msg)
        {
            try
            {
                socket.SendMoreFrame(Encoding.UTF8.GetBytes(topic)).SendMoreFrame(msg[0]).SendMoreFrame(msg[1]).SendFrame(msg[2]);
                //NetMQMessage NMmsg = new NetMQMessage();
                //NMmsg.Append(topic);
                //NMmsg.Append(msg[0]);
                //NMmsg.Append(msg[1]);
                //NMmsg.Append(msg[2]);
                //socket.TrySendMultipartMessage(NMmsg);
            }
            catch (Exception ex)
            {
                _errlog.Error($"Zmq SendData: {ex.Message}\r\n{ex.StackTrace}\r\n{ex.Source}");
            }
        }

        private void AddQueue(string topic)
        {
            Queue<byte[][]> _Q = new Queue<byte[][]>();
            if (!socketQueue.Keys.Contains(topic))
            {
                socketQueue.TryAdd(topic, _Q);
                Console.WriteLine($"AddQueue: {topic}");
            }
        }
    }
}
