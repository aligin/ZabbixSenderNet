using System;
using System.Collections.Generic;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;
using ZabbixSenderNet.Model;

namespace ZabbixSenderNet
{
    public class ZabbixSender
    {
        #region Properties
        /// <summary>
        /// DefaultTimeOut in miliseconds
        /// </summary>
        private const int DefaultTimeOut = 3000;
        //implementing the Zabbix Protocol https://www.zabbix.com/documentation/1.8/protocols
        private static byte[] Header = Encoding.ASCII.GetBytes("ZBXD\x01");

        public string Host { get; set; } = "127.0.0.1";
        public int Port { get; set; } = 10051;
        public int ConnectionTimeout { get; set; } = DefaultTimeOut;
        public int SocketTimeout { get; set; } = DefaultTimeOut;
        #endregion

        #region Constructors
        /// <summary>
        /// Initializes a new instance of the <see cref="ZabbixSender"/> class.
        /// </summary>
        /// <param name="host">The host.</param>
        /// <param name="port">The port.</param>
        public ZabbixSender(String host, int port)
            : this(host, port, DefaultTimeOut, DefaultTimeOut)
        {

        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ZabbixSender"/> class.
        /// </summary>
        /// <param name="host">The host.</param>
        /// <param name="port">The port.</param>
        /// <param name="connectionTimeout">The connection timeout in miliseconds.</param>
        /// <param name="socketTimeout">The socket timeout in miliseconds.</param>
        public ZabbixSender(String host, int port, int connectionTimeout, int socketTimeout)
        {
            Host = host;
            Port = port;
            ConnectionTimeout = connectionTimeout;
            SocketTimeout = socketTimeout;
        }
        #endregion

        #region Methods

        /// <summary>
        /// Sends the specified host.
        /// </summary>
        /// <param name="host">The host.</param>
        /// <param name="key">The key.</param>
        /// <param name="value">The value.</param>
        /// <returns></returns>
        public async Task<ZabbixResult> Send(string host, string key, string value)
        {
            var result = await Send(new List<ZabbixSenderItem> { new ZabbixSenderItem(host, key, value) });

            return result;
        }

        /// <summary>
        /// Sends the specified items.
        /// </summary>
        /// <param name="items">The items.</param>
        /// <returns></returns>
        public async Task<ZabbixResult> Send(List<ZabbixSenderItem> items)
        {
            var jsonObject = new
            {
                request = "sender data",
                data = items
            };

            var result =  await Send(JsonConvert.SerializeObject(jsonObject, new JsonSerializerSettings
            {
                ContractResolver = new CamelCasePropertyNamesContractResolver()
            }));

            return result;
        }

        /// <summary>
        /// Sends the specified json message.
        /// </summary>
        /// <param name="jsonMessage">The json message.</param>
        /// <returns></returns>
        protected async Task<ZabbixResult> Send(string jsonMessage)
        {
            ZabbixResult result = new ZabbixResult();

            try
            {
                byte[] length = BitConverter.GetBytes((long)jsonMessage.Length);
                byte[] jsonData = Encoding.ASCII.GetBytes(jsonMessage);

                //initialize array of Byte
                byte[] all = new byte[Header.Length + length.Length + jsonData.Length];

                System.Buffer.BlockCopy(Header, 0, all, 0, Header.Length);
                System.Buffer.BlockCopy(length, 0, all, Header.Length, length.Length);
                System.Buffer.BlockCopy(jsonData, 0, all, Header.Length + length.Length, jsonData.Length);

                using (var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp))
                {
                    socket.ReceiveTimeout = SocketTimeout;
                    socket.SendTimeout = SocketTimeout;
                    await socket.ConnectAsync(Host, Port);

                    // Send To Zabbix
                    var data = new ArraySegment<Byte>(all);
                    await socket.SendAsync(data, SocketFlags.None);

                    result = await GetSocketResponse(socket);
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }

            return result;
        }

        /// <summary>
        /// Gets the socket response.
        /// </summary>
        /// <param name="socket">The socket.</param>
        /// <returns></returns>
        /// <exception cref="Exception">
        /// Invalid response
        /// or
        /// Invalid response
        /// </exception>
        private async Task<ZabbixResult> GetSocketResponse(Socket socket)
        {
            //Receive Response Header
            var responseHeader = new ArraySegment<Byte>(new Byte[5]);
            await GetSocketResponse(socket, responseHeader);

            if ("ZBXD\x01" != Encoding.ASCII.GetString(responseHeader.Array, responseHeader.Offset, responseHeader.Count))
                throw new Exception("Invalid response");

            // Receive Data Length
            var responseDataLength = new ArraySegment<Byte>(new Byte[8]);
            await GetSocketResponse(socket, responseDataLength);
            int dataLength = BitConverter.ToInt32(responseDataLength.Array, responseDataLength.Offset);

            if (dataLength == 0)
                throw new Exception("Invalid response");

            // Receive Response Message
            var responseMessage = new ArraySegment<Byte>(new Byte[dataLength]);
            await GetSocketResponse(socket, responseMessage);

            string jsonResponse = Encoding.ASCII.GetString(responseMessage.Array, responseMessage.Offset, responseMessage.Count);

            return JsonConvert.DeserializeObject<ZabbixResult>(jsonResponse, new JsonSerializerSettings
            {
                ContractResolver = new CamelCasePropertyNamesContractResolver()
            });
        }

        private async Task GetSocketResponse(Socket socket, ArraySegment<byte> buffer)
        {
            await socket.ReceiveAsync(buffer, SocketFlags.None);
        }
        #endregion
    }
}
