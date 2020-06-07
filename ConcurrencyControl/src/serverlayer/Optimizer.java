package serverlayer;

import dbms.SystemCatalog;
import serverlayer.model.LogicalPlan;
import serverlayer.model.PhysicalPlan;
import serverlayer.model.Predicate;

public class Optimizer {

    public PhysicalPlan convertToPhysicalPlan(LogicalPlan logicalPlan) {
        Predicate secondary = null;
        for (Predicate p : logicalPlan.getPredicates()) {
            if (p.getOffset() == 0) {
                return new PhysicalPlan(p, logicalPlan);
            } else if (SystemCatalog.getTableConfig(logicalPlan.getTable()).containsSecondaryIndex(p.getOffset())) {
                secondary = p;
            }
        }
        return new PhysicalPlan(secondary, logicalPlan);
    }
}
