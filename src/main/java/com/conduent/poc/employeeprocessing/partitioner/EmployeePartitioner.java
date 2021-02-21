package com.conduent.poc.employeeprocessing.partitioner;

import com.conduent.poc.employeeprocessing.model.Employee;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;

public class EmployeePartitioner implements Partitioner{
    private static final Logger logger = LoggerFactory.getLogger(EmployeePartitioner.class);

    FlatFileItemReader<Employee> employeeItemReader;

    List<Employee> employeeList;

    public EmployeePartitioner(FlatFileItemReader<Employee> employeeItemReader)
    {
        this.employeeItemReader=employeeItemReader;
    }

    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        Map<String,ExecutionContext> partitionerContext=new HashMap<>();
        List<List<Employee>> partitionedEmployeeLists=new ArrayList<>();
        for (int count=0;count<employeeList.size();count+=gridSize) {
            int end = Math.min(employeeList.size(), count+gridSize);
            List<Employee> partition=new ArrayList<>();
            partition.addAll(employeeList.subList(count, end));
            partitionedEmployeeLists.add(partition);
        }
        for(int count=0;count<partitionedEmployeeLists.size();count++) {
            ExecutionContext executionContext = new ExecutionContext();
            logger.info("partition:" + count + "size:"+partitionedEmployeeLists.get(count).size());
            executionContext.put("employeeList",partitionedEmployeeLists.get(count));
            executionContext.put("ThreadName:",String.valueOf(count));
            partitionerContext.put("partition: " + count, executionContext);
        }
        return partitionerContext;
    }

    @BeforeStep
    public void beforePartition(StepExecution stepExecution)throws Exception
    {
        employeeList=new ArrayList<>();
        try {
            employeeItemReader.open(stepExecution.getExecutionContext());
            while (true) {
                Employee employee=employeeItemReader.read();
                if(employee==null){
                    break;
                }else {
                    employeeList.add(employee);
                }
            }
        }
        catch (Exception exception){
            logger.error("Exception occurred while reading employee csv file",exception);
            throw exception;
        }
        finally {
            employeeItemReader.close();
        }

    }
}
