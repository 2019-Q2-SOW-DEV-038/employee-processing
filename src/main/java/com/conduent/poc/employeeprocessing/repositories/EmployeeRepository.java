package com.conduent.poc.employeeprocessing.repositories;

import com.conduent.poc.employeeprocessing.model.Employee;
import org.springframework.data.jpa.repository.JpaRepository;

public interface EmployeeRepository extends JpaRepository<Employee,Integer> {
}
