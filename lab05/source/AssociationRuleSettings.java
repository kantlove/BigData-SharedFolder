package association_rule;

public class AssociationRuleSettings {
	public static int number_of_lines = 0;
	public static final float minsup = 0.01f;
	public static final float minconf = 0.7f;
	
	public static int minsup_limit() {
		return (int)(minsup * number_of_lines);
	}
}
