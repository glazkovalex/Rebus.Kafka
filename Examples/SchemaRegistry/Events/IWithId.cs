namespace SchemaRegistry.Events
{
    internal interface IWithId
    {
        public Guid Id { get; }
    }
}
