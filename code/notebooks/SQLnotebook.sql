-- Databricks notebook source
-- 创建表
CREATE TABLE employees (
  id INT,
  name STRING,
  age INT,
  department STRING
);

-- 插入数据
INSERT INTO employees VALUES
  (1, 'John Doe', 30, 'IT'),
  (2, 'Jane Smith', 28, 'HR'),
  (3, 'Mike Johnson', 35, 'Finance');


-- COMMAND ----------

-- 查询所有员工
SELECT * FROM employees;

-- 按部门查询员工
SELECT * FROM employees WHERE department = 'IT';

-- 按年龄排序查询员工
SELECT * FROM employees ORDER BY age DESC;

