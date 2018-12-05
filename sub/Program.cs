using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NetMQ;
using NetMQ.Sockets;
using Jsunfutures.Messages;
using Google.Protobuf;
using ZooKeeperNet;
using System.IO;
using System.Threading;
using System.Collections.Concurrent;
using log4net;

namespace OmsnfManageMapping
{
    public struct Accounts
    {
        public List<string> account;
        public List<string> seaAccount;
    }
    class Program
    {
        static private ILog _mainlog = LogManager.GetLogger("Main");
        static private ILog _errlog = LogManager.GetLogger("Error");
        static private ILog _userlog = LogManager.GetLogger("Userdata");
        static private ZooKeeper zk;
        static private string pathFile_api;
        static private string pathFile_morninglight;
        static public string version = string.Empty;
        static ZmqSocket zmqS = new ZmqSocket();
        static private ConcurrentDictionary<string, Accounts> dic_sso_api = new ConcurrentDictionary<string, Accounts>();
        static private ConcurrentDictionary<string, Accounts> dic_sso_morninglight = new ConcurrentDictionary<string, Accounts>();
        static void Main(string[] args)
        {
            //File Path
            string pathFolder = Properties.Settings.Default.folderPath;
            Directory.CreateDirectory(pathFolder);
            pathFile_api = pathFolder + "\\IDandAccountMapping_api.txt";
            pathFile_morninglight = pathFolder + "\\IDandAccountMapping_morninglight.txt";

            //Get Initial Mapping
            dic_sso_api = initDic(pathFile_api);
            dic_sso_morninglight = initDic(pathFile_morninglight);

            //Zookeeper
            string _ZkpEndPoint = Properties.Settings.Default.ZooKeeperPath;
            Console.WriteLine($"ZooKeeperPath: {_ZkpEndPoint}");
            try
            {
                zk = new ZooKeeper(_ZkpEndPoint, new TimeSpan(0, 0, 0, 1000), new Watcher());
            }
            catch(Exception ex)
            {
                _errlog.Error($"Zookeeper Error: {ex.Message}\r\n{ex.StackTrace}\r\n{ex.Source}");
            }
            Thread.Sleep(100);

            //NetMQ
            string _topic = Properties.Settings.Default.topic;
            string _topicPath = Properties.Settings.Default.topicPath;
            string[] splTopic, topic_tcp;

            try
            {
                foreach (string topicName in _topic.Split('|'))
                {
                    splTopic = topicName.Split('.');
                    topic_tcp = Encoding.UTF8.GetString(zk.GetData(_topicPath + "/" + topicName, false, null)).Split(',');

                    switch (splTopic[0])
                    {
                        case "OmsnfOrs":
                            if (splTopic[1].Contains("Pub") && splTopic[2].Contains("MsgUserdata"))
                            {
                                zmqS.Start(ZmqSocketType.Sub, false, topicName, topic_tcp);
                            }
                            break;
                        case "OmsManageMapping":
                            if (splTopic[1].Contains("Pub") && splTopic[4].Contains("OmsManageSystem"))
                            {
                                zmqS.Start(ZmqSocketType.Pub, true, topicName, topic_tcp);
                            }
                            break;
                        case "OmsManageSystem":
                            if (splTopic[3].Contains("Sub") && splTopic[4].Contains("OmsManageMapping"))
                            {
                                zmqS.Start(ZmqSocketType.Sub, true, topicName, topic_tcp);
                            }
                            break;
                    }
                }
            }
            catch(Exception ex)
            {
                _errlog.Error($"Topic Error: {ex.Message}\r\n{ex.StackTrace}\r\n{ex.Source}");
            }

            try
            {
                //ManageUserData
                while (true)
                {
                    if (!zmqS.socketQueue.ContainsKey("OmsnfOrs.ZfPub.MsgUserdata.ZfSub.OmsnfQueryserver"))
                        Thread.Sleep(100);
                    else
                        break;
                }
                porcessManageUserData(zmqS.socketQueue["OmsnfOrs.ZfPub.MsgUserdata.ZfSub.OmsnfQueryserver"]);
                Console.WriteLine("Start porcessManageUserData");
                _mainlog.Info("Start porcessManageUserData:OmsnfOrs.ZfPub.MsgUserdata.ZfSub.OmsnfQueryserver");
            }
            catch(Exception ex)
            {
                _errlog.Error($"ManageUserData Error: {ex.Message}\r\n{ex.StackTrace}\r\n{ex.Source}");
            }

            try
            {
                //Get OmsManageSystem Initial Request and Pub Mapping
                string topic1 = "OmsManageSystem.ZfPub.MsgRequest.ZfSub.OmsManageMapping";
                while (true)
                {
                    if (!zmqS.socketQueue.ContainsKey(topic1))
                        Thread.Sleep(100);
                    else
                        break;
                }
                porcessManageMapping(zmqS.socketQueue[topic1], zmqS.socketDictionary[topic1], zmqS, topic1);
                Console.WriteLine("Start porcessManageMapping " + topic1);
                _mainlog.Info($"Start porcessManageUserData:{topic1}");
            }
            catch(Exception ex)
            {
                _errlog.Error($"Get OmsManageSystem Initial Request Error: {ex.Message}\r\n{ex.StackTrace}\r\n{ex.Source}");
            }

            try
            {
                //Get OmsManageSystem Update Mapping 
                string topic2 = "OmsManageSystem.ZfPub.MsgMapping.ZfSub.OmsManageMapping";
                while (true)
                {
                    if (!zmqS.socketQueue.ContainsKey(topic2))
                        Thread.Sleep(100);
                    else
                        break;
                }
                porcessManageMapping(zmqS.socketQueue[topic2], zmqS.socketDictionary[topic2], zmqS, topic2);
                Console.WriteLine("Start porcessManageMapping " + topic2);
                _mainlog.Info($"Start porcessManageUserData:{topic2}");
            }
            catch(Exception ex)
            {
                _errlog.Error($"OmsManageSystem Update Mapping Error: {ex.Message}\r\n{ex.StackTrace}\r\n{ex.Source}");
            }

            Console.Read();
        }

        static public void porcessManageMapping(Queue<byte[][]> _Q, NetMQSocket socket, ZmqSocket zmqS, string topic)
        {
            try
            {
                Task.Run(() =>
                {
                    while (true)
                    {
                        if (_Q.Count != 0)
                        {
                            byte[][] reciveData = _Q.Dequeue();
                            
                            string[] reciveTopic = Encoding.UTF8.GetString(reciveData[0]).Split('.');
                            if (reciveTopic.Count() >= 2)
                            {
                                switch (reciveTopic[2])
                                {
                                    case "MsgRequest":
                                        byte[][] msg1 = new byte[3][];
                                        version = DateTime.Now.ToString("yyyyMMddHHmmssffffff");
                                        msg1[0] = Encoding.UTF8.GetBytes(version);
                                        msg1[1] = File2Byte(pathFile_api);
                                        msg1[2] = File2Byte(pathFile_morninglight);

                                        zmqS.SendData(zmqS.socketDictionary["OmsManageMapping.ZfPub.MsgInitMapping.ZfSub.OmsManageSystem"], "OmsManageMapping.ZfPub.MsgInitMapping.ZfSub.OmsManageSystem", msg1);
                                        _mainlog.Info($"Send MsgInitMapping, version={version}");
                                        break;
                                    case "MsgMapping":
                                        string tmpPath = string.Empty;
                                        version = Encoding.UTF8.GetString(reciveData[1]);
                                        string clientFlag = Encoding.UTF8.GetString(reciveData[2]);
                                        string strMapping = Encoding.UTF8.GetString(reciveData[3]);
                                        _mainlog.Info($"reciveData={strMapping}, clientFlag={clientFlag}, version={version}");
                                        string[] tmp = strMapping.Split('|');

                                        if (clientFlag == "API")
                                            tmpPath = pathFile_api;
                                        else if (clientFlag == "Morninglight")
                                            tmpPath = pathFile_morninglight;

                                        using (StreamWriter sw = new StreamWriter(new FileStream(tmpPath, FileMode.Append, FileAccess.Write)))
                                        {
                                            sw.WriteLine(strMapping);
                                            Console.WriteLine("write:" + strMapping);
                                            _mainlog.Info($"write:{strMapping}");
                                        }

                                        byte[][] msg2 = new byte[3][];
                                        msg2[0] = reciveData[1];
                                        msg2[1] = reciveData[2];
                                        msg2[2] = File2Byte(tmpPath);
                                        zmqS.SendData(zmqS.socketDictionary["OmsManageMapping.ZfPub.MsgMapping.ZfSub.OmsManageSystem"], "OmsManageMapping.ZfPub.MsgMapping.ZfSub.OmsManageSystem", msg2);
                                        _mainlog.Info($"Send MsgMapping, version={version}");
                                        break;
                                }
                            }
                        }
                        Thread.Sleep(10);
                    }
                });
            }
            catch(Exception ex)
            {
                _errlog.Error($"porcessManageMapping Error: {ex.Message}\r\n{ex.StackTrace}\r\n{ex.Source}");
            }
        }

        static public void porcessManageUserData(Queue<byte[][]> _Q)
        {
            try
            {
                Task.Run(() =>
                {
                    while (true)
                    {
                        Accounts acc = new Accounts();

                        if (_Q.Count != 0)
                        {
                            byte[][] subData = _Q.Dequeue();
                            SSO s = SSO.Parser.ParseFrom(subData[1]);
                            _mainlog.Info($"SSO: ClientFlag={s.Cf}, ID={s.ID}, ToTalAccount={s.ToTalAccount}");

                            switch (s.Cf)
                            {
                                case ClientFlag.CfCustomize:
                                    manageFiles(s, dic_sso_api, acc, pathFile_api);
                                    break;
                                case ClientFlag.CfMorninglight:
                                    manageFiles(s, dic_sso_morninglight, acc, pathFile_morninglight);
                                    break;
                            }
                        }
                        Thread.Sleep(10);
                    }
                });
            }
            catch (Exception ex)
            {
                _errlog.Error($"porcessManageUserData Error: {ex.Message}\r\n{ex.StackTrace}\r\n{ex.Source}");
            }
        }

        static private void manageFiles(SSO s, ConcurrentDictionary<string, Accounts> dic_sso, Accounts acc, string pathFile)
        {
            try
            {
                string[] splitData = s.ToTalAccount.Split(';');
                acc = addList(splitData);

                bool changeFlag = false;
                if (dic_sso.ContainsKey(s.ID))
                {
                    IEnumerable<string> difaccount = dic_sso[s.ID].account.Except(acc.account);
                    IEnumerable<string> difaccount2 = acc.account.Except(dic_sso[s.ID].account);
                    IEnumerable<string> difseaAccount = dic_sso[s.ID].seaAccount.Except(acc.seaAccount);
                    IEnumerable<string> difseaAccount2 = acc.seaAccount.Except(dic_sso[s.ID].seaAccount);
                    if (difaccount.Count() != 0 || difseaAccount.Count() != 0 || difaccount2.Count() != 0 || difseaAccount2.Count() != 0)
                    {
                        changeFlag = true;
                        string toWrite = string.Empty;
                        //update file
                        using (StreamReader sr = new StreamReader(pathFile))
                        {
                            string AllLine = sr.ReadToEnd();
                            string[] strline = AllLine.Split('\n');
                            foreach (string line in strline)
                            {
                                if (line != "\n")
                                {
                                    if (line.Split('|')[0] == s.ID)
                                    {
                                        string tmpline = s.ID + "|" + s.ToTalAccount + "\r\n";
                                        toWrite += tmpline;
                                    }
                                    else
                                    {
                                        toWrite += line + '\n';
                                    }
                                }
                            }
                        }
                        using (StreamWriter sw = new StreamWriter(new FileStream(pathFile, FileMode.Create, FileAccess.Write)))
                        {
                            sw.Write(toWrite);
                            Console.WriteLine("ReWrite:" + pathFile);
                            _mainlog.Info($"ReWrite: {pathFile}");
                        }
                    }
                }
                else
                {
                    changeFlag = true;
                    using (StreamWriter sw = new StreamWriter(new FileStream(pathFile, FileMode.Append, FileAccess.Write)))
                    {
                        sw.WriteLine(s.ID + "|" + s.ToTalAccount);
                        Console.WriteLine("write:" + s.ID + "|" + s.ToTalAccount);
                        _mainlog.Info($"Write {pathFile}: {s.ID}|{s.ToTalAccount}");
                    }
                }

                if (changeFlag) //to do:add version 
                {
                    dic_sso.AddOrUpdate(s.ID, acc, (k, v) => acc);

                    //do pub to OMSManageSystem MsgMapping
                    byte[][] msg = new byte[3][];

                    version = DateTime.Now.ToString("yyyyMMddHHmmssffffff");
                    msg[0] = Encoding.UTF8.GetBytes(version);
                    if (s.Cf == ClientFlag.CfCustomize)
                        msg[1] = Encoding.UTF8.GetBytes("API");
                    else if (s.Cf == ClientFlag.CfMorninglight)
                        msg[1] = Encoding.UTF8.GetBytes("Morninglight");
                    msg[2] = File2Byte(pathFile);
                    zmqS.SendData(zmqS.socketDictionary["OmsManageMapping.ZfPub.MsgMapping.ZfSub.OmsManageSystem"], "OmsManageMapping.ZfPub.MsgMapping.ZfSub.OmsManageSystem", msg);
                    _mainlog.Info("Send MsgMapping");
                }
                Thread.Sleep(10);
            }
            catch (Exception ex)
            {
                _errlog.Error($"manageFiles Error: {ex.Message}\r\n{ex.StackTrace}\r\n{ex.Source}");
            }
        }

        static private byte[] File2Byte(string FilePath)
        {
            try
            {
                string AllLine;
                using (StreamReader sr = new StreamReader(FilePath))
                {
                    AllLine = sr.ReadToEnd();
                }
                byte[] msg = Encoding.UTF8.GetBytes(AllLine);

                return msg;
            }
            catch(Exception ex)
            {
                _errlog.Error($"File2Byte Error: {ex.Message}\r\n{ex.StackTrace}\r\n{ex.Source}");
                return null;
            }
        }

        static private ConcurrentDictionary<string, Accounts> initDic(string pathFile)
        {
            ConcurrentDictionary<string, Accounts> dic_sso = new ConcurrentDictionary<string, Accounts>();

            try
            {
                if (File.Exists(pathFile))
                {
                    using (StreamReader sr = new StreamReader(pathFile))
                    {
                        string AllLine = sr.ReadToEnd();
                        string[] strline = AllLine.Split('\n');

                        foreach (string str in strline)
                        {
                            string tmp = str.TrimEnd('\r');
                            string[] idORaccount = tmp.Split('|');
                            if (idORaccount.Count() > 1)
                            {
                                string[] splitData = idORaccount[1].Split(';');
                                Accounts acc = new Accounts();
                                acc = addList(splitData);
                                dic_sso.AddOrUpdate(idORaccount[0], acc, (k, v) => acc);
                            }
                        }
                    }
                }
            }
            catch(Exception ex)
            {
                _errlog.Error($"initDic Error: {ex.Message}\r\n{ex.StackTrace}\r\n{ex.Source}");
            }
            return dic_sso;
        }

        static private Accounts addList(string[] splitData)
        {
            Accounts acc = new Accounts();
            acc.account = new List<string>();
            acc.seaAccount = new List<string>();

            try
            {
                for (int i = 0; i < splitData.Count(); i++)
                {
                    string[] splitAccountDetail = splitData[i].Split(',');
                    if (splitAccountDetail[0] == "02")
                    {
                        acc.account.Add(splitAccountDetail[2]);
                    }
                    else if (splitAccountDetail[0] == "05")
                    {
                        acc.seaAccount.Add(splitAccountDetail[2]);
                    }
                }
            }
            catch(Exception ex)
            {
                _errlog.Error($"addList Error: {ex.Message}\r\n{ex.StackTrace}\r\n{ex.Source}");
            }
            return acc;
        }
    }
    class Watcher : IWatcher
    {
        public void Process(WatchedEvent @event) { }
    }
}
