namespace Thorium.Data.Implementation
{
    public class SqlCondition : SqlValue
    {
        public SqlCondition(string column, object shouldBe) : base(column, shouldBe)
        {

        }

        public object ShouldBe { get { return Value; } set { Value = value; } }
    }
}
