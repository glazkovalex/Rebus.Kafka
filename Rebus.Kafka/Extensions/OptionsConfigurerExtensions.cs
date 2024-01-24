using Rebus.Config;
using Rebus.Topic;
using System;
using System.Collections.Concurrent;
using System.Linq;

namespace Rebus.Kafka
{
    /// <summary>
    /// Extensions fot <see cref="OptionsConfigurer"/>
    /// </summary>
    public static class OptionsConfigurerExtensions
    {
        /// <summary>
        /// Simplifies naming the topic of events to "---Topic---.&lt;Spacename&gt;.&lt;TypeName&gt;".
        /// </summary>
        /// <param name="configurer"></param>
        [Obsolete("Using the " + nameof(UseAttributeOrTypeFullNameForTopicNames))]
        public static void UseNamespaceAndTypeTopicNames(this OptionsConfigurer configurer)
        {
            configurer.Decorate<ITopicNameConvention>(c => new ShortTopicFullNameConvention());
        }

        /// <summary>
        /// Simplifies naming the topic of events by Name from TopicAttribute(&lt;TopicName&gt;) or to "---Topic---.&lt;Spacename&gt;.&lt;TypeName&gt;".
        /// </summary>
        /// <param name="configurer"></param>
        public static void UseAttributeOrTypeFullNameForTopicNames(this OptionsConfigurer configurer)
        {
            configurer.Decorate<ITopicNameConvention>(c => new ShortTopicAttributeOrFullNameConvention());
        }

        class ShortTopicFullNameConvention : ITopicNameConvention
        {
            public string GetTopic(Type type) => type.FullName;
        }

        class ShortTopicAttributeOrFullNameConvention : ITopicNameConvention
        {
            static ConcurrentDictionary<Type, string> _caсhe = new ConcurrentDictionary<Type, string>();

            public string GetTopic(Type type)
            {
                return _caсhe.GetOrAdd(type
                    , (type.GetCustomAttributes(typeof(TopicAttribute), true).FirstOrDefault() as TopicAttribute)?.Name ?? type.FullName);
            }
        }
    }

    /// <summary>
    /// An attribute for naming the topic of events in Apache Kafka
    /// </summary>
    [AttributeUsage(AttributeTargets.Class, Inherited = false)]
    public class TopicAttribute : Attribute
    {
        public string Name { get; set; }
        public TopicAttribute(string name)
        {
            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentNullException("name");
            Name = name;
        }
    }
}
