package com.demo.service;

import java.util.List;

import com.demo.entity.User;
import com.github.pagehelper.PageInfo;

public interface UserService extends BaseService<User>{

    public List<User> getUserList();

    public User findUserById(long id);

    public void save(User user);

    public void edit(User user);

    public void delete(long id);
    
    public PageInfo<User> getPageWithMybatis(Integer currPageNum, Integer currPageSize);


}
