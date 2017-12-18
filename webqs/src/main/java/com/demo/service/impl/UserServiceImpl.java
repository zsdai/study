package com.demo.service.impl;

import java.io.Serializable;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Service;

import com.demo.entity.User;
import com.demo.mapper.UserMapper;
import com.demo.repository.UserRepository;
import com.demo.service.UserService;
import com.github.pagehelper.PageHelper;
import com.github.pagehelper.PageInfo;

@Service
public class UserServiceImpl extends BaseServiceImpl<User> implements UserService{

	@Override
	public JpaRepository<User, Serializable> getRepository() {
		return (JpaRepository)userRepository;
	}
	
    @Autowired
    private UserRepository userRepository;

    @Autowired
    private UserMapper userMapper;
    
    
    /**  
    * @Title: getPageWithMybatis 
    * @author 代祯山  
    * @Description:mabatis 分页
    * @param currPageNum
    * @param currPageSize
    * @return
    */
    public PageInfo<User> getPageWithMybatis(Integer currPageNum, Integer currPageSize){
    	 PageHelper.startPage(currPageNum, currPageSize);
    	 List<User> userList = userMapper.getAll();
    	return new PageInfo<>(userList);
    }
    
    @Override
    public List<User> getUserList() {
        return userRepository.findAll();
    }

    @Override
    public User findUserById(long id) {
        return userRepository.findById(id);
    }

    @Override
    public void save(User user) {
        userRepository.save(user);
    }

    @Override
    public void edit(User user) {
        userRepository.save(user);
    }

    @Override
    public void delete(long id) {
        userRepository.delete(id);
    }


}


