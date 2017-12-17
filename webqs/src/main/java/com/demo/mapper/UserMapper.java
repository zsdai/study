package com.demo.mapper;

import java.util.List;

import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Result;
import org.apache.ibatis.annotations.Results;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import com.demo.entity.User;

public interface UserMapper {
	
	@Select("SELECT * FROM user")
	@Results({
		@Result(property = "userName",  column = "user_name", javaType = String.class)
	})
	List<User> getAll();
	
	@Select("SELECT * FROM user WHERE id = #{id}")
	@Results({
		@Result(property = "userName",  column = "user_name", javaType = String.class)
	})
	User getOne(Long id);

	@Insert("INSERT INTO user(user_name,password,age) VALUES(#{userName}, #{password}, #{age})")
	void insert(User user);

	@Update("UPDATE user SET user_name=#{userName},age=#{age} WHERE id =#{id}")
	void update(User user);

	@Delete("DELETE FROM user WHERE id =#{id}")
	void delete(Long id);

}