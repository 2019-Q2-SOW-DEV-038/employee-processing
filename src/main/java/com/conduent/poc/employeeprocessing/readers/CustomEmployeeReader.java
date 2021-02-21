package com.conduent.poc.employeeprocessing.readers;

import com.conduent.poc.employeeprocessing.model.Employee;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;

import java.util.List;

public class CustomEmployeeReader implements ItemReader<Employee> {

    private List<Employee> employeeList;
    int currentIndex=0;

    public CustomEmployeeReader(List<Employee> employees){
        this.employeeList=employees;
    }

    @Override
    public Employee read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
        Employee employee=null;
        if(currentIndex<employeeList.size()){
            employee=employeeList.get(currentIndex);
            currentIndex++;
        }
        return employee;
    }
}
