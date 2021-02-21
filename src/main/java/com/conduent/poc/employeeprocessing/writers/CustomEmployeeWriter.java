package com.conduent.poc.employeeprocessing.writers;

import com.conduent.poc.employeeprocessing.model.Employee;
import com.conduent.poc.employeeprocessing.repositories.EmployeeRepository;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

public class CustomEmployeeWriter implements ItemWriter<Employee> {

    @Autowired
    private EmployeeRepository employeeRepository;

    @Override
    public void write(List<? extends Employee> employees) throws Exception {
        employeeRepository.saveAll(employees);
    }
}
