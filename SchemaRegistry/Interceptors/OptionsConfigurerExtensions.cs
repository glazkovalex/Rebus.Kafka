using Rebus.Config;
using Rebus.Pipeline;
using Rebus.Pipeline.Send;

namespace SchemaRegistry.Interceptors
{
    internal static class OptionsConfigurerExtensions
    {
        /// <summary>
        /// Insert the step before adding the kafka temporary header before the serialization step
        /// </summary>
        /// <param name="configurer"></param>
        /// <param name="outgoingStep">Custom interceptor</param>
        public static void InsertStepAfterAutoHeadersOutgoingStep(this OptionsConfigurer configurer, IOutgoingStep outgoingStep)
        {
            configurer.Decorate<IPipeline>(c =>
            {
                var pipeline = c.Get<IPipeline>();
                return new PipelineStepInjector(pipeline)
                    .OnSend(outgoingStep, PipelineRelativePosition.After, typeof(AutoHeadersOutgoingStep));
            });
        }
    }
}
