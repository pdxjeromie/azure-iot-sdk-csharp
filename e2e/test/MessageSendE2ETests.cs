// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Diagnostics.Tracing;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Devices.Client;
using Microsoft.Azure.Devices.Client.Exceptions;
using Microsoft.Azure.Devices.Client.Transport.Mqtt;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Microsoft.Azure.Devices.E2ETests
{
    [TestClass]
    [TestCategory("E2E")]
    [TestCategory("IoTHub")]
    public partial class MessageSendE2ETests : IDisposable
    {
        private const int BatchMessageCount = 5;

        // Max message size is 256kb for IoT Hub, but the transport buffer contains protocol information along with the payload
        private const int LargeMessageSize = 1024 * 255;

        private readonly string _devicePrefix = $"E2E_{nameof(MessageSendE2ETests)}_";
        private readonly string _modulePrefix = $"E2E_{nameof(MessageSendE2ETests)}_";
        private static readonly string s_proxyServerAddress = Configuration.IoTHub.ProxyServerAddress;
        private static readonly TestLogging s_log = TestLogging.GetInstance();

        private readonly ConsoleEventListener _listener = TestConfig.StartEventListener();

        [DataTestMethod]
        [DataRow(TestDeviceType.Sasl, Client.TransportType.Amqp_Tcp_Only)]
        [DataRow(TestDeviceType.Sasl, Client.TransportType.Amqp_WebSocket_Only)]
        [DataRow(TestDeviceType.Sasl, Client.TransportType.Mqtt_Tcp_Only)]
        [DataRow(TestDeviceType.Sasl, Client.TransportType.Mqtt_WebSocket_Only)]
        [DataRow(TestDeviceType.Sasl, Client.TransportType.Http1)]
        [DataRow(TestDeviceType.X509, Client.TransportType.Amqp_Tcp_Only)]
        [DataRow(TestDeviceType.X509, Client.TransportType.Amqp_WebSocket_Only)]
        [DataRow(TestDeviceType.X509, Client.TransportType.Mqtt_Tcp_Only)]
        [DataRow(TestDeviceType.X509, Client.TransportType.Mqtt_WebSocket_Only)]
        [DataRow(TestDeviceType.X509, Client.TransportType.Http1)]
        public async Task DeviceClient_SendSingleMessageAsync(TestDeviceType testDeviceType, Client.TransportType transportType)
        {
            await SendSingleMessageAsync(testDeviceType, transportType).ConfigureAwait(false);
        }

        [DataTestMethod]
        [DataRow(Client.TransportType.Amqp_Tcp_Only)]
        [DataRow(Client.TransportType.Amqp_WebSocket_Only)]
        public async Task DeviceClient_WithAmqpHeartbeat_SendSingleMessageAsync(Client.TransportType transportType)
        {
            var amqpTransportSettings = new AmqpTransportSettings(transportType)
            {
                IdleTimeout = TimeSpan.FromMinutes(2)
            };
            var transportSettings = new ITransportSettings[] { amqpTransportSettings };
            await SendSingleMessageAsync(TestDeviceType.Sasl, transportSettings).ConfigureAwait(false);
        }

        [TestCategory("Proxy")]
        [TestCategory("LongRunning")]
        [DataTestMethod]
        [DataRow(TestDeviceType.Sasl)]
        [DataRow(TestDeviceType.X509)]
        public async Task DeviceClient_WithHttpProxy_SendSingleMessageAsync(TestDeviceType testDeviceType)
        {
            var httpTransportSettings = new Http1TransportSettings
            {
                Proxy = new WebProxy(s_proxyServerAddress)
            };
            var transportSettings = new ITransportSettings[] { httpTransportSettings };

            await SendSingleMessageAsync(testDeviceType, transportSettings).ConfigureAwait(false);
        }

        [TestCategory("Proxy")]
        [DataTestMethod]
        [DataRow(TestDeviceType.Sasl)]
        [DataRow(TestDeviceType.X509)]
        public async Task DeviceClient_WithCustomProxy_SendSingleMessageAsync(TestDeviceType testDeviceType)
        {
            var httpTransportSettings = new Http1TransportSettings();
            var proxy = new CustomWebProxy();
            httpTransportSettings.Proxy = proxy;
            var transportSettings = new ITransportSettings[] { httpTransportSettings };

            await SendSingleMessageAsync(testDeviceType, transportSettings).ConfigureAwait(false);
            Assert.AreNotEqual(proxy.Counter, 0);
        }

        [TestCategory("Proxy")]
        [TestCategory("LongRunning")]
        [DataTestMethod]
        [DataRow(TestDeviceType.Sasl)]
        [DataRow(TestDeviceType.X509)]
        public async Task DeviceClient_WithAmqpWsProxy_SendSingleMessageAsync(TestDeviceType testDeviceType)
        {
            var amqpTransportSettings = new AmqpTransportSettings(Client.TransportType.Amqp_WebSocket_Only)
            {
                Proxy = new WebProxy(s_proxyServerAddress)
            };
            var transportSettings = new ITransportSettings[] { amqpTransportSettings };

            await SendSingleMessageAsync(testDeviceType, transportSettings).ConfigureAwait(false);
        }

        [TestCategory("Proxy")]
        [DataTestMethod]
        [DataRow(TestDeviceType.Sasl)]
        [DataRow(TestDeviceType.X509)]
        public async Task DeviceClient_WithMqttWsProxy_SendSingleMessageAsync(TestDeviceType testDeviceType)
        {
            var mqttTransportSettings = new MqttTransportSettings(Client.TransportType.Mqtt_WebSocket_Only)
            {
                Proxy = new WebProxy(s_proxyServerAddress)
            };
            var transportSettings = new ITransportSettings[] { mqttTransportSettings };

            await SendSingleMessageAsync(testDeviceType, transportSettings).ConfigureAwait(false);
        }

        [TestMethod]
        [TestCategory("Proxy")]
        public async Task ModuleClient_WithAmqpWsProxy_SendSingleMessageAsync()
        {
            var amqpTransportSettings = new AmqpTransportSettings(Client.TransportType.Amqp_WebSocket_Only)
            {
                Proxy = new WebProxy(s_proxyServerAddress)
            };
            var transportSettings = new ITransportSettings[] { amqpTransportSettings };

            await SendSingleMessageModuleAsync(transportSettings).ConfigureAwait(false);
        }

        [TestMethod]
        [TestCategory("Proxy")]
        public async Task ModuleClient_WithMqttWsProxy_SendSingleMessageAsync()
        {
            MqttTransportSettings mqttTransportSettings = new MqttTransportSettings(Client.TransportType.Mqtt_WebSocket_Only)
            {
                Proxy = new WebProxy(s_proxyServerAddress)
            };
            ITransportSettings[] transportSettings = new ITransportSettings[] { mqttTransportSettings };

            await SendSingleMessageModuleAsync(transportSettings).ConfigureAwait(false);
        }

        [DataTestMethod]
        [DataRow(TestDeviceType.Sasl, Client.TransportType.Amqp_Tcp_Only)]
        [DataRow(TestDeviceType.Sasl, Client.TransportType.Amqp_WebSocket_Only)]
        [DataRow(TestDeviceType.Sasl, Client.TransportType.Mqtt_Tcp_Only)]
        [DataRow(TestDeviceType.Sasl, Client.TransportType.Mqtt_WebSocket_Only)]
        [DataRow(TestDeviceType.Sasl, Client.TransportType.Http1)]
        [DataRow(TestDeviceType.X509, Client.TransportType.Amqp_Tcp_Only)]
        [DataRow(TestDeviceType.X509, Client.TransportType.Amqp_WebSocket_Only)]
        [DataRow(TestDeviceType.X509, Client.TransportType.Mqtt_Tcp_Only)]
        [DataRow(TestDeviceType.X509, Client.TransportType.Mqtt_WebSocket_Only)]
        [DataRow(TestDeviceType.X509, Client.TransportType.Http1)]
        public async Task DeviceClient_SendBatchMessagesAsync(TestDeviceType testDeviceType, Client.TransportType transportType)
        {
            await SendBatchMessagesAsync(testDeviceType, transportType).ConfigureAwait(false);
        }

        [DataTestMethod]
        [DataRow(TestDeviceType.Sasl, Client.TransportType.Amqp_Tcp_Only)]
        [DataRow(TestDeviceType.Sasl, Client.TransportType.Amqp_WebSocket_Only)]
        [DataRow(TestDeviceType.Sasl, Client.TransportType.Mqtt_Tcp_Only)]
        [DataRow(TestDeviceType.Sasl, Client.TransportType.Mqtt_WebSocket_Only)]
        [DataRow(TestDeviceType.Sasl, Client.TransportType.Http1)]
        [DataRow(TestDeviceType.X509, Client.TransportType.Amqp_Tcp_Only)]
        [DataRow(TestDeviceType.X509, Client.TransportType.Amqp_WebSocket_Only)]
        [DataRow(TestDeviceType.X509, Client.TransportType.Mqtt_Tcp_Only)]
        [DataRow(TestDeviceType.X509, Client.TransportType.Mqtt_WebSocket_Only)]
        [DataRow(TestDeviceType.X509, Client.TransportType.Http1)]
        public async Task DeviceClient_SendLargeMessageAsync(TestDeviceType testDeviceType, Client.TransportType transportType)
        {
            TestDevice testDevice = await TestDevice.GetTestDeviceAsync(_devicePrefix, testDeviceType).ConfigureAwait(false);
            using DeviceClient deviceClient = testDevice.CreateDeviceClient(transportType);

            using var message = new Client.Message(Encoding.UTF8.GetBytes(new string('*', LargeMessageSize)));
            await deviceClient.SendEventAsync(message).ConfigureAwait(false);
        }

        [TestMethod]
        [ExpectedException(typeof(MessageTooLargeException))]
        public async Task DeviceClient_LargeTopicName_ThrowsException_SendMessageAsync()
        {
            TestDevice testDevice = await TestDevice.GetTestDeviceAsync(_devicePrefix).ConfigureAwait(false);
            using DeviceClient deviceClient = testDevice.CreateDeviceClient(Client.TransportType.Mqtt);

            await deviceClient.OpenAsync().ConfigureAwait(false);

            using var msg = new Client.Message(Encoding.UTF8.GetBytes("testMessage"));
            //Mqtt topic name consists of, among other things, system properties and user properties
            // setting lots of very long user properties should cause a MessageTooLargeException explaining
            // that the topic name is too long to publish over mqtt
            for (int i = 0; i < 100; i++)
            {
                msg.Properties.Add(Guid.NewGuid().ToString(), new string('1', 1024));
            }

            await deviceClient.SendEventAsync(msg).ConfigureAwait(false);
        }

        private async Task SendSingleMessageAsync(TestDeviceType type, Client.TransportType transport)
        {
            TestDevice testDevice = await TestDevice.GetTestDeviceAsync(_devicePrefix, type).ConfigureAwait(false);
            using DeviceClient deviceClient = testDevice.CreateDeviceClient(transport);

            await deviceClient.OpenAsync().ConfigureAwait(false);
            await SendSingleMessageAndVerifyAsync(deviceClient, testDevice.Id).ConfigureAwait(false);
            await deviceClient.CloseAsync().ConfigureAwait(false);
        }

        private async Task SendBatchMessagesAsync(TestDeviceType type, Client.TransportType transport)
        {
            TestDevice testDevice = await TestDevice.GetTestDeviceAsync(_devicePrefix, type).ConfigureAwait(false);
            using DeviceClient deviceClient = testDevice.CreateDeviceClient(transport);

            await deviceClient.OpenAsync().ConfigureAwait(false);
            await SendSendBatchMessagesAndVerifyAsync(deviceClient, testDevice.Id).ConfigureAwait(false);
            await deviceClient.CloseAsync().ConfigureAwait(false);
        }

        private async Task SendSingleMessageAsync(TestDeviceType type, ITransportSettings[] transportSettings)
        {
            TestDevice testDevice = await TestDevice.GetTestDeviceAsync(_devicePrefix, type).ConfigureAwait(false);
            using DeviceClient deviceClient = testDevice.CreateDeviceClient(transportSettings);

            await deviceClient.OpenAsync().ConfigureAwait(false);
            await SendSingleMessageAndVerifyAsync(deviceClient, testDevice.Id).ConfigureAwait(false);
            await deviceClient.CloseAsync().ConfigureAwait(false);
        }

        private async Task SendSingleMessageModuleAsync(ITransportSettings[] transportSettings)
        {
            TestModule testModule = await TestModule.GetTestModuleAsync(_devicePrefix, _modulePrefix).ConfigureAwait(false);
            using var moduleClient = ModuleClient.CreateFromConnectionString(testModule.ConnectionString, transportSettings);

            await moduleClient.OpenAsync().ConfigureAwait(false);
            await SendSingleMessageModuleAndVerifyAsync(moduleClient, testModule.DeviceId).ConfigureAwait(false);
            await moduleClient.CloseAsync().ConfigureAwait(false);
        }

        public static async Task SendSingleMessageAndVerifyAsync(DeviceClient deviceClient, string deviceId)
        {
            (Client.Message testMessage, _, string payload, string p1Value) = ComposeD2cTestMessage();

            using (testMessage)
            {
                await deviceClient.SendEventAsync(testMessage).ConfigureAwait(false);

                bool isReceived = EventHubTestListener.VerifyIfMessageIsReceived(deviceId, payload, p1Value);
                Assert.IsTrue(isReceived, "Message is not received.");
            }
        }

        public static async Task SendSendBatchMessagesAndVerifyAsync(DeviceClient deviceClient, string deviceId)
        {
            var messages = new List<Client.Message>();

            try
            {
                var props = new List<Tuple<string, string>>();
                for (int i = 0; i < BatchMessageCount; i++)
                {
                    (Client.Message testMessage, string messageId, string payload, string p1Value) = ComposeD2cTestMessage();
                    messages.Add(testMessage);
                    props.Add(Tuple.Create(payload, p1Value));
                }

                await deviceClient.SendEventBatchAsync(messages).ConfigureAwait(false);

                foreach (Tuple<string, string> prop in props)
                {
                    bool isReceived = EventHubTestListener.VerifyIfMessageIsReceived(deviceId, prop.Item1, prop.Item2);
                    Assert.IsTrue(isReceived, "Message is not received.");
                }
            }
            finally
            {
                foreach (Client.Message message in messages)
                {
                    message.Dispose();
                }
            }
        }

        private async Task SendSingleMessageModuleAndVerifyAsync(ModuleClient moduleClient, string deviceId)
        {
            (Client.Message testMessage, _, string payload, string p1Value) = ComposeD2cTestMessage();

            using (testMessage)
            {
                await moduleClient.SendEventAsync(testMessage).ConfigureAwait(false);

                bool isReceived = EventHubTestListener.VerifyIfMessageIsReceived(deviceId, payload, p1Value);
                Assert.IsTrue(isReceived, "Message is not received.");
            }
        }

        public static (Client.Message message, string messageId, string payload, string p1Value) ComposeD2cTestMessage()
        {
            string messageId = Guid.NewGuid().ToString();
            string payload = Guid.NewGuid().ToString();
            string p1Value = Guid.NewGuid().ToString();

            s_log.WriteLine($"{nameof(ComposeD2cTestMessage)}: messageId='{messageId}' payload='{payload}' p1Value='{p1Value}'");
            var message = new Client.Message(Encoding.UTF8.GetBytes(payload))
            {
                MessageId = messageId,
            };
            message.Properties.Add("property1", p1Value);
            message.Properties.Add("property2", null);

            return (message, messageId, payload, p1Value);
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
        }
    }
}
