package com.demo.service;

import java.util.List;

import com.demo.entity.User;

public interface UserService {

    public List<User> getUserList();

    public User findUserById(long id);

    public void save(User user);

    public void edit(User user);

    public void delete(long id);


}
