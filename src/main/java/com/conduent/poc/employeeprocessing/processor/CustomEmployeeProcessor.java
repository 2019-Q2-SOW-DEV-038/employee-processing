package com.conduent.poc.employeeprocessing.processor;

import com.conduent.poc.employeeprocessing.model.Employee;
import com.conduent.poc.employeeprocessing.partitioner.EmployeePartitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;

public class CustomEmployeeProcessor implements ItemProcessor<Employee, Employee> {

    private static final Logger logger = LoggerFactory.getLogger(CustomEmployeeProcessor.class);
    private final String threadName;

    public CustomEmployeeProcessor(String threadName){
        this.threadName=threadName;
    }

    @Override
    public Employee process(Employee employee) throws Exception {
        logger.info("Executing processor in thread"+threadName);
        return Employee.builder().employeeId(employee.getEmployeeId())
                .firstName(employee.getFirstName().toUpperCase())
                .lastName(employee.getLastName().toUpperCase())
                .build();
    }
}
