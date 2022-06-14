package com.zuehlke.securesoftwaredevelopment.repository;

import com.zuehlke.securesoftwaredevelopment.config.AuditLogger;
import com.zuehlke.securesoftwaredevelopment.config.Entity;
import com.zuehlke.securesoftwaredevelopment.domain.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

@Repository
public class CustomerRepository {

    private static final Logger LOG = LoggerFactory.getLogger(CustomerRepository.class);
    private static final AuditLogger auditLogger = AuditLogger.getAuditLogger(CustomerRepository.class);

    private DataSource dataSource;

    public CustomerRepository(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    private Person createPersonFromResultSet(ResultSet rs) throws SQLException {
        int id = rs.getInt(1);
        String firstName = rs.getString(2);
        String lastName = rs.getString(3);
        String personalNumber = rs.getString(4);
        String address = rs.getString(5);
        LOG.info("New person successfully created");
        return new Person(id, firstName, lastName, personalNumber, address);
    }

    public List<Customer> getCustomers() {
        List<com.zuehlke.securesoftwaredevelopment.domain.Customer> customers = new ArrayList<com.zuehlke.securesoftwaredevelopment.domain.Customer>();
        String query = "SELECT id, username FROM users";
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(query)) {

            while (rs.next()) {
                customers.add(createCustomer(rs));
            }
            LOG.info("Customers successfully pulled from database");
        } catch (SQLException e) {
            LOG.warn("Customers unsuccessfully pulled from database", e);
        }
        return customers;
    }

    private com.zuehlke.securesoftwaredevelopment.domain.Customer createCustomer(ResultSet rs) throws SQLException {
        return new com.zuehlke.securesoftwaredevelopment.domain.Customer(rs.getInt(1), rs.getString(2));
    }

    public List<Restaurant> getRestaurants() {
        List<Restaurant> restaurants = new ArrayList<Restaurant>();
        String query = "SELECT r.id, r.name, r.address, rt.name  FROM restaurant AS r JOIN restaurant_type AS rt ON r.typeId = rt.id ";
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(query)) {

            while (rs.next()) {
                restaurants.add(createRestaurant(rs));
            }
            LOG.info("Restaurants successfully pulled from database");
        } catch (SQLException e) {
            LOG.warn("Customers unsuccessfully pulled from database!", e);
        }
        return restaurants;
    }

    private Restaurant createRestaurant(ResultSet rs) throws SQLException {
        int id = rs.getInt(1);
        String name = rs.getString(2);
        String address = rs.getString(3);
        String type = rs.getString(4);
        LOG.info("New restaurant successfully created");
        return new Restaurant(id, name, address, type);
    }


    public Object getRestaurant(String id) {
        String query = "SELECT r.id, r.name, r.address, rt.name  FROM restaurant AS r JOIN restaurant_type AS rt ON r.typeId = rt.id WHERE r.id=" + id;
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(query)) {

            if (rs.next()) {
                LOG.info("Restaurant successfully created with given id:" + id);
                return createRestaurant(rs);
            }
            LOG.info("Restaurant with given id:" + id + "cannot be created");
        } catch (SQLException e) {
            LOG.warn("Restaurant not created!", e);
        }
        return null;
    }

    public void deleteRestaurant(int id) {
        String query = "DELETE FROM restaurant WHERE id=" + id;
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()
        ) {
            statement.executeUpdate(query);
            LOG.info("Restaurant successfully deleted");
        } catch (SQLException e) {
            LOG.warn("Restaurant unsuccessfully deleted", e);
        }
    }

    public void updateRestaurant(RestaurantUpdate restaurantUpdate) {
        Restaurant restaurantFromDb = (Restaurant) getRestaurant(String.valueOf(restaurantUpdate.getId()));
        String query = "UPDATE restaurant SET name = '" + restaurantUpdate.getName() + "', address='" + restaurantUpdate.getAddress() + "', typeId =" + restaurantUpdate.getRestaurantType() + " WHERE id =" + restaurantUpdate.getId();
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()
        ) {
            statement.executeUpdate(query);
            AuditLogger.getAuditLogger(CustomerRepository.class).
                    auditChange(new Entity("restaurant.update",
                            String.valueOf(restaurantUpdate.getId()),
                            "Name: "+restaurantFromDb.getName()+" Address:" + restaurantFromDb.getAddress() + " Type: " + restaurantFromDb.getRestaurantType(),
                            "Name: "+restaurantUpdate.getName()+" Address:" + restaurantUpdate.getAddress() + " Type: " + restaurantUpdate.getRestaurantType()));
        } catch (SQLException e) {
            LOG.warn("Restaurant unsuccessfully updated", e);
        }

    }

    public Customer getCustomer(String id) {
        String sqlQuery = "SELECT id, username, password FROM users WHERE id=" + id;
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(sqlQuery)) {

            if (rs.next()) {
                LOG.info("Customer with given id: " + id + " successfully pulled from database");
                return createCustomerWithPassword(rs);
            }
            LOG.info("There is no customer with given id: " + id + " in database");

        } catch (SQLException e) {
            LOG.warn("Customer unsuccessfully pulled from database", e);
        }
        return null;
    }

    private Customer createCustomerWithPassword(ResultSet rs) throws SQLException {
        int id = rs.getInt(1);
        String username = rs.getString(2);
        String password = rs.getString(3);
        LOG.info("New customer successfully created");
        return new Customer(id, username, password);
    }


    public void deleteCustomer(String id) {
        String query = "DELETE FROM users WHERE id=" + id;
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()
        ) {
            statement.executeUpdate(query);
            LOG.info("Customer successfully deleted");
        } catch (SQLException e) {
            LOG.warn("Customer unsuccessfully deleted", e);
        }
    }

    public void updateCustomer(CustomerUpdate customerUpdate) {
        Customer customerFromDb = getCustomer(String.valueOf(customerUpdate.getId()));//Da li sme ovo?
        String query = "UPDATE users SET username = '" + customerUpdate.getUsername() + "', password='" + customerUpdate.getPassword() + "' WHERE id =" + customerUpdate.getId();
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()
        ) {
            statement.executeUpdate(query);
            AuditLogger.getAuditLogger(CustomerRepository.class).
                    auditChange(new Entity("customer.update",
                            String.valueOf(customerUpdate.getId()),
                            "Username: " + customerFromDb.getUsername(),
                            "Username: " + customerUpdate.getUsername()));
        } catch (SQLException e) {
            LOG.warn("Customer unsuccessfully updated", e);
        }
    }

    public List<Address> getAddresses(String id) {
        String sqlQuery = "SELECT id, name FROM address WHERE userId=" + id;
        List<Address> addresses = new ArrayList<Address>();
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(sqlQuery)) {

            while (rs.next()) {
                addresses.add(createAddress(rs));
            }
            LOG.info("Addresses successfully pulled from database");

        } catch (SQLException e) {
            LOG.warn("Addresses unsuccessfully pulled from database", e);
        }
        return addresses;
    }

    private Address createAddress(ResultSet rs) throws SQLException {
        int id = rs.getInt(1);
        String name = rs.getString(2);
        LOG.info("New address successfully created");
        return new Address(id, name);
    }

    public void deleteCustomerAddress(int id) {
        String query = "DELETE FROM address WHERE id=" + id;
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()
        ) {
            statement.executeUpdate(query);
            LOG.info("Customer with given id: " + id + " successfully deleted");
        } catch (SQLException e) {
            LOG.warn("Customer with given id: " + id + " successfully deleted", e);
        }
    }

    public void updateCustomerAddress(Address address) {
        List<Address> addressList = getAddresses(String.valueOf(address.getId()));
        String query = "UPDATE address SET name = '" + address.getName() + "' WHERE id =" + address.getId();
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()
        ) {
            statement.executeUpdate(query);
            for(int i = 0; i < addressList.size(); i++){
                auditLogger.auditChange(new Entity("customerAddress.update", String.valueOf(addressList.get(0).getId()),
                        "Address name: " + addressList.get(0).getName(),
                        "Address name: " + addressList.get(0).getName()));
            }
        } catch (SQLException e) {
            LOG.warn("Unsuccessfully updated customer address with name: " + address.getName(), e);
        }
    }

    public void putCustomerAddress(NewAddress newAddress) {
        String query = "INSERT INTO address (name, userId) VALUES ('"+newAddress.getName()+"' , "+newAddress.getUserId()+")";
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()
        ) {
            statement.executeUpdate(query);
            LOG.info("New address has been successfully added for customer: " + newAddress.getUserId());
        } catch (SQLException e) {
            LOG.info("New address has been unsuccessfully added for customer: " + newAddress.getUserId());
        }
    }
}
