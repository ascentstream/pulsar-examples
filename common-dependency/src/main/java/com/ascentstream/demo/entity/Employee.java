package com.ascentstream.demo.entity;

public class Employee extends User {

    private String title;

    public Employee() {
    }

    public Employee(String ID, String name, Integer age) {
        super(ID, name, age);
    }

    public Employee(String ID, String name, Integer age, String title) {
        super(ID, name, age);
        this.title = title;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }
}
