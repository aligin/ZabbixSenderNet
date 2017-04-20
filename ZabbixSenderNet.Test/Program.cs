﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ZabbixSenderNet.Test
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.OutputEncoding = Encoding.UTF8;
            try
            {
                string zabbixIp = "10.120.50.10";
                int zabbixPort = 10051;

                string key = "MyTestKey";
                string host = "MyTestHost";


                ZabbixSender sender = new ZabbixSender(zabbixIp, zabbixPort);

                //This is when we send ourvalue "0" to zabbix
                var result = sender.Send(host, key, "0").GetAwaiter().GetResult();

                Console.WriteLine("success : {0}, processed : {1}, failed : {2}, total : {3}, seconds: {4}",
                    result.IsSuccess,
                    result.ZabbixResultInfo.Processed,
                    result.ZabbixResultInfo.Failed,
                    result.ZabbixResultInfo.Total,
                    result.ZabbixResultInfo.SpentSeconds);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
            finally
            {
                Console.ReadKey();
            }
        }
    }
}
