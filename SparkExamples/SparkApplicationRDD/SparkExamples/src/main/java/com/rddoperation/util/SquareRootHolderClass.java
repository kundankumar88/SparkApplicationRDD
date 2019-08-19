package com.rddoperation.util;

public class SquareRootHolderClass {
	
	private Double mainNumber;
	Double squareRoot;
	
	public SquareRootHolderClass()
	{
		
	}
	
	public SquareRootHolderClass(Double mainNumber)
	{
		this.mainNumber=mainNumber;
		this.squareRoot= Math.sqrt(mainNumber);
		
	}

	public Double getMainNumber() {
		return mainNumber;
	}

	public void setMainNumber(Double mainNumber) {
		this.mainNumber = mainNumber;
	}

	public Double getSquareRoot() {
		return squareRoot;
	}

	public void setSquareRoot(Double squareRoot) {
		this.squareRoot = squareRoot;
	}
	
	
	
	

}
