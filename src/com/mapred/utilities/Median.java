package com.mapred.utilities;

public class Median {
	public double findMedian(double[] a,int left,int right){ 
        double index = 0d; 
        int mid = (left+right)/2; 
        index = partition(a,left,right); 
        while( index != mid){ 
            if(index < mid) 
                index = partition(a,mid,right); 
            else index = partition(a,left,mid); 
        } 
        return index; 
    } 
    public double partition(double[] a,int i,int j ){ 
        int pivot = (i+j)/2; 
        double temp; 
        while(i <= j){ 
            while(a[i] < a[pivot]) 
                i++; 
            while(a[j] > a[pivot]) 
                j--; 
            if(i <= j){ 
                temp = a[i]; 
                a[i]=a[j]; 
                a[j] = temp; 
                i++;j--; 
            } 
        } 
        return pivot; 
    } 

}
