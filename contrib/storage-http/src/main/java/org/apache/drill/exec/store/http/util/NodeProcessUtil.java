package org.apache.drill.exec.store.http.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.planner.logical.DrillScanRel;
import org.apache.drill.exec.planner.physical.Prel;
import org.apache.drill.exec.planner.physical.ScanPrel;

public class NodeProcessUtil {
	
	
	
	public static List<String> getOrderBycols(final Prel sort) {
		
		List<RexNode> orderByCols = sort.getChildExps();
		List<String> orderFields = new ArrayList<String>();
		List<RelCollation> collationList = sort.getCollationList();
		
		for (RelCollation relCollation : collationList) {
			
			List<RelFieldCollation>  relFieldCollationList = relCollation.getFieldCollations();
			for (RelFieldCollation relFieldCollation : relFieldCollationList) {
				
				relFieldCollation.getFieldIndex();
				relFieldCollation.getDirection();
				
			}

		}
		
		for (RexNode rexNode : orderByCols) {

			if (rexNode instanceof RexCall) {

				RexCall tmpProjectChildExp = (RexCall) rexNode;

				// TODO currently don't support orderby function push down
				return null;

			} else if (rexNode instanceof RexInputRef) {

				RexInputRef tmpProjectChildExp = (RexInputRef) rexNode;

				orderFields.add(sort.getRowType().getFieldList().get(tmpProjectChildExp.getIndex()).getName());
			} else {
				// unknow push down
				return null;
			}

		}
		return orderFields;
	}

	public static GroupScan findGroupScan(RelNode relNode) {

		if (relNode == null) {
			return null;
		}

		GroupScan scan = null;

		if (relNode instanceof ScanPrel) {

			return ((ScanPrel)relNode).getGroupScan();
		}else if (relNode instanceof DrillScanRel) {
			
			return ((DrillScanRel)relNode).getGroupScan();
		}else if (relNode instanceof RelSubset) {
			RelNode bestNode = ((RelSubset) relNode).getBest();
			if(bestNode == null){
				List<RelNode> relNodes = ((RelSubset) relNode).getRelList();
				if (relNodes == null || relNodes.size() == 0) {
					return null;
				}
				return findGroupScan(relNodes.get(0));
			} else {
				return findGroupScan(bestNode);
			}
		} else {

			List<RelNode> relNodes = relNode.getInputs();
			if (relNodes == null || relNodes.size() == 0) {
				scan = null;
			}
			scan = findScanFromRelNodeList(relNodes);
			
			

		}

		return scan;
	}

	public static GroupScan findScanFromRelNodeList(List<RelNode> relNodes) {
		
		GroupScan scan = null;
		
		for (RelNode node : relNodes) {
			scan = findGroupScan(node);
			if(scan != null){
				
				return scan;
			}
		}
		return scan;
	}
	
	
	/*
	public static ScanPrel findScanPrel(RelNode relNode) {

		if (relNode == null) {
			return null;
		}

		ScanPrel scan = null;

		if (relNode instanceof ScanPrel) {

			return (ScanPrel)relNode;
		} else if (relNode instanceof RelSubset) {
			RelNode bestNode = ((RelSubset) relNode).getBest();
			if(bestNode == null){
				List<RelNode> relNodes = ((RelSubset) relNode).getRelList();
				if (relNodes == null || relNodes.size() == 0) {
					return null;
				}
				return findScanPrel(relNodes.get(0));
			} else {
				return findScanPrel(bestNode);
			}
		} else {

			List<RelNode> relNodes = relNode.getInputs();
			if (relNodes == null || relNodes.size() == 0) {
				scan = null;
			}
			scan = findScanFromRelNodeList(relNodes);
			
			

		}

		return scan;
	}

	public static ScanPrel findScanFromRelNodeList(List<RelNode> relNodes) {
		
		ScanPrel scan = null;
		
		for (RelNode node : relNodes) {
			scan = findScanPrel(node);
			if(scan != null){
				
				return scan;
			}
		}
		return scan;
	}*/

}
