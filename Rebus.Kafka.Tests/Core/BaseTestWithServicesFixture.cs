using System;
using Xunit;
using Xunit.Abstractions;

namespace Rebus.Kafka.Tests.Core
{
    /// <summary>
    /// Base class for test classes that require capturing
    /// </summary>
    [Collection("ServicesFixture")]
    public abstract class BaseTestWithServicesFixture : IClassFixture<ServicesFixture>, IDisposable
    {
        protected ServicesFixture Fixture;
        protected ITestOutputHelper Output;

        protected BaseTestWithServicesFixture(ServicesFixture fixture, ITestOutputHelper output)
        {
            Fixture = fixture;
            Output = output;
        }

        public void Dispose()
        {
            Output.WriteLine("\nOutput logs created ServicesFixture at startup:\n");
            Fixture.Report(Output);
        }
    }
}
