namespace Rebus.Kafka.Core
{
    internal class Result
    {
        internal bool Sucsess { get; }
        internal bool Failure => !Sucsess;
        internal string Reason { get; }

        internal static Result Ok() => new Result(true, null);
        internal static Result Fail(string reason) => new Result(false, reason);

        private Result(bool sucsess, string reason)
        {
            Sucsess = sucsess;
            Reason = reason;
        }
    }
}
