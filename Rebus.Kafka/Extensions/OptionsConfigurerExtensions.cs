using Rebus.Config;
using Rebus.Topic;
using System;

namespace Rebus.Kafka
{
    /// <summary>
    /// Extensions fot <see cref="OptionsConfigurer"/>
    /// </summary>
    public static class OptionsConfigurerExtensions
    {
        /// <summary>
        /// Simplifies event names to "---Topic---.&lt;Spacename&gt;.&lt;TypeName&gt;"
        /// </summary>
        /// <param name="configurer"></param>
        public static void UseNamespaceAndTypeTopicNames(this OptionsConfigurer configurer)
        {
            configurer.Decorate<ITopicNameConvention>(c => new ShortTopicNamesConvention());
        }

        class ShortTopicNamesConvention : ITopicNameConvention
        {
            public string GetTopic(Type type) => $"{type.Namespace}.{type.Name}";
        }
    }
}
