using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using Docker.DotNet;
using Docker.DotNet.Models;
using Xunit;
using Xunit.Abstractions;

namespace Rebus.Kafka.Tests.Core
{
    public class ServicesFixture : IDisposable
    {
        public ServicesFixture()
        {
            //docker run -p 2181:2181 -p 9092:9092 --env ADVERTISED_HOST=127.0.0.1 --env ADVERTISED_PORT=9092 --name kafka-rebus spotify/kafka
            _output.Add("Starting One time setup");
            var client = new DockerClientConfiguration(LocalDockerUri()).CreateClient();
            var listCont = client.Containers.ListContainersAsync(new ContainersListParameters()).Result;
            _output.Add($"I found {listCont.Count} running containers on the local machine");
            // the docker images are started, need to restart them
            foreach (var x in listCont.Where(x => x.Names.Any(y => y.Contains("kafka-rebus"))))
            {
                bool completed = client.Containers.StopContainerAsync(x.ID, new ContainerStopParameters()).Result;
                client.Containers.RemoveContainerAsync(x.ID, new ContainerRemoveParameters()).Wait();
            }

            // pull image again
            client.Images.CreateImageAsync(
                new ImagesCreateParameters
                {
                    FromImage = "spotify/kafka",
                    Tag = "latest"
                },
                new AuthConfig(),
                new Progress<JSONMessage>(msg =>
                    _output.Add($"{msg.Status}|{msg.ProgressMessage}|{msg.ErrorMessage}")
                )).Wait();

            var parameters = new Docker.DotNet.Models.Config
            {
                Image = "spotify/kafka:latest",
                ArgsEscaped = true,
                AttachStderr = true,
                AttachStdin = true,
                AttachStdout = true,
                Env = new List<string> { "ADVERTISED_PORT=9092", "ADVERTISED_HOST=127.0.0.1" }
            };

            var ports2181 = new List<PortBinding> { new PortBinding { HostPort = "2181", HostIP = "" } };
            var ports9092 = new List<PortBinding> { new PortBinding { HostPort = "9092", HostIP = "" } };

            var resp = client.Containers.CreateContainerAsync(new CreateContainerParameters(parameters)
            {
                HostConfig = new HostConfig { PortBindings = new Dictionary<string, IList<PortBinding>>() { { "2181/tcp", ports2181 }, { "9092/tcp", ports9092 } } },
                Name = "kafka-rebus"
            }).Result;
            bool result = client.Containers.StartContainerAsync(resp.ID, new ContainerStartParameters()).Result;

            Thread.Sleep(3000);
        }

        public void Report(ITestOutputHelper output)
        {
            foreach (var log in _output)
                output.WriteLine(log);
        }

#if NET46
        public static bool isWindows = true;
#else
        public static bool isWindows = RuntimeInformation.IsOSPlatform(OSPlatform.Windows);
#endif
        private Uri LocalDockerUri()
        {
            return isWindows ? new Uri("npipe://./pipe/docker_engine") : new Uri("unix:///var/run/docker.sock");
        }

        public readonly string KafkaEndpoint = "127.0.0.1:9092";

        private readonly List<string> _output = new List<string>();

        public void Dispose()
        {
            // clean up test data from the database
        }

        [CollectionDefinition("ServicesFixture")]
        public class TestFixtureCollection : ICollectionFixture<ServicesFixture>
        {
            // This class has no code, and is never created. Its purpose is simply
            // to be the place to apply [CollectionDefinition] and all the
            // ICollectionFixture<> interfaces.
        }
    }
}
