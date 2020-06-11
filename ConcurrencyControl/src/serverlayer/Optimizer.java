package serverlayer;

import dbms.SystemCatalog;
import dbms.TableSystemCatalog;
import serverlayer.model.LogicalPlan;
import serverlayer.model.PhysicalPlan;
import serverlayer.model.Predicate;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Optimizer {

    public PhysicalPlan convertToPhysicalPlan(LogicalPlan logicalPlan) {
        Map<Integer, List<Predicate>> offset2Predicates = new HashMap<>();
        TableSystemCatalog tableConfig = SystemCatalog.getTableConfig(logicalPlan.getTable());
        for (Predicate p : logicalPlan.getPredicates()) {
            if (p.getOffset() == 0 || tableConfig.containsSecondaryIndex(p.getOffset())) {
                offset2Predicates.computeIfAbsent(p.getOffset(), y -> new ArrayList<>()).add(p);
            }
        }
        List<Predicate> selected = Collections.emptyList();
        if (offset2Predicates.containsKey(0)) {
            selected = offset2Predicates.get(0);
        } else if (!offset2Predicates.isEmpty()) {
            selected = offset2Predicates.entrySet().iterator().next().getValue();
        }
        Collections.sort(selected, (a, b)->
                Integer.compare(b.expression.getIndexSelectPriority(), a.expression.getIndexSelectPriority()));
        return new PhysicalPlan(selected, logicalPlan);
    }
}
