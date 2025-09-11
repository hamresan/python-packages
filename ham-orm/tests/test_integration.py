"""
Integration tests for ham-orm package.
These tests demonstrate real usage patterns.
"""
import pytest
from unittest.mock import Mock, MagicMock
from ham_orm import AppBaseModel, QueryBuilder, dualmethod


# Mock SQLAlchemy classes for integration testing
class MockUser:
    """Mock SQLAlchemy User model."""
    __name__ = "User"
    __tablename__ = "users"
    
    def __init__(self):
        self.id = None
        self.name = None
        self.email = None
        self.age = None
        self.__dict__["_sa_instance_state"] = Mock()


class UserModel(AppBaseModel):
    """User model extending AppBaseModel."""
    _model = MockUser
    _primary_key = "id"
    _guard_fields = ["email"]  # Protect email from mass assignment
    

class TestIntegration:
    """Integration tests for the ham-orm package."""
    
    @pytest.fixture
    def mock_db_session(self):
        """Create a comprehensive mock database session."""
        session = Mock()
        session.add = Mock()
        session.commit = Mock()
        session.rollback = Mock()
        session.refresh = Mock()
        session.query = Mock()  # For legacy support
        return session

    def test_model_crud_workflow(self, mock_db_session):
        """Test complete CRUD workflow with the model."""
        # Initialize the model with database
        UserModel.init_db(mock_db_session)
        
        # Test 1: Create new user (insert)
        user_data = {"name": "John Doe", "age": 30}
        
        # Mock the behavior for successful insert
        with pytest.patch.object(UserModel, '_store') as mock_store:
            mock_store.return_value = UserModel()
            new_user = UserModel.insert(user_data)
            assert new_user is not None
            mock_store.assert_called_once_with(user_data, is_updating=False, is_saving=False)
        
        # Test 2: Find user by ID
        with pytest.patch.object(UserModel, 'first') as mock_first:
            mock_user_entity = MockUser()
            mock_user_entity.id = 1
            mock_user_entity.name = "John Doe"
            mock_first.return_value = UserModel(entity=mock_user_entity)
            
            found_user = UserModel.find(1)
            assert found_user is not None
            assert found_user.name == "John Doe"
        
        # Test 3: Update user
        update_data = {"name": "John Smith"}
        with pytest.patch.object(UserModel, '_store') as mock_store:
            mock_store.return_value = UserModel()
            updated_user = UserModel.update(update_data)
            assert updated_user is not None
            mock_store.assert_called_once_with(update_data, is_updating=True, is_saving=False)

    def test_query_builder_integration(self, mock_db_session):
        """Test QueryBuilder integration with various query patterns."""
        # Create QueryBuilder instance
        qb = QueryBuilder(mock_db_session, MockUser)
        
        # Test chaining methods
        query_builder = (qb
                        .where({"name": "John", "age__gte": 18})
                        .order_by("-id", "name")
                        .limit(10)
                        .offset(5)
                        .include("profile")
                        .only("id", "name", "email"))
        
        # Verify it's still the same instance (for chaining)
        assert query_builder is qb
        
        # Check internal state
        assert qb._limit == 10
        assert qb._offset == 5
        assert len(qb._order_by) == 2  # -id and name
        assert len(qb._includes) == 1  # profile
        assert len(qb._only_cols) == 3  # id, name, email

    def test_dualmethod_integration(self):
        """Test dualmethod decorator in real usage scenarios."""
        
        class TestModel(AppBaseModel):
            _model = MockUser
            
            @dualmethod
            def custom_operation(self):
                return f"Operation on {self._model.__name__}"
        
        # Test calling on instance
        instance = TestModel()
        result_instance = instance.custom_operation()
        assert result_instance == "Operation on User"
        
        # Test calling on class (should create new instance)
        result_class = TestModel.custom_operation()
        assert result_class == "Operation on User"

    def test_model_validation_and_error_handling(self, mock_db_session):
        """Test validation and error handling scenarios."""
        UserModel.init_db(mock_db_session)
        
        # Test 1: Model without _model should raise error
        class InvalidModel(AppBaseModel):
            _model = None
        
        with pytest.raises(RuntimeError, match="Model is not initialized"):
            InvalidModel()
        
        # Test 2: Database operations without initialized session
        UserModel._db = None
        with pytest.raises(RuntimeError, match="Database session is not initialized"):
            UserModel.first()
        
        # Restore session for other tests
        UserModel._db = mock_db_session

    def test_field_protection_and_whitelisting(self):
        """Test field protection (guard fields) and whitelisting."""
        
        class ProtectedUserModel(AppBaseModel):
            _model = MockUser
            _guard_fields = ["email", "password"]
            _whitelist_fields = ["name", "age"]  # Only allow these fields
        
        model = ProtectedUserModel()
        
        # Test guard fields are protected
        model._entity.email = "original@example.com"
        data_with_protected = {
            "name": "John",
            "email": "hacker@example.com",  # Should be ignored
            "age": 25
        }
        
        model.populate(data_with_protected)
        assert model._entity.name == "John"
        assert model._entity.age == 25
        assert model._entity.email == "original@example.com"  # Should remain unchanged

    def test_custom_hooks_workflow(self, mock_db_session):
        """Test custom before/after hooks in the workflow."""
        
        class CustomUserModel(AppBaseModel):
            _model = MockUser
            
            def before_insert(self, data):
                # Add timestamp or modify data before insert
                data["created_at"] = "2024-01-01"
                return data
            
            def after_insert(self):
                # Perform post-insert operations
                return True  # Success
            
            def before_update(self, data):
                # Add updated timestamp
                data["updated_at"] = "2024-01-02"
                return data
            
            def after_update(self, old):
                # Perform post-update operations
                return True  # Success
        
        CustomUserModel.init_db(mock_db_session)
        model = CustomUserModel()
        
        # Test hooks are called during store operations
        with pytest.patch.object(model, 'before_insert', return_value={"name": "test"}) as mock_before, \
             pytest.patch.object(model, 'after_insert', return_value=True) as mock_after, \
             pytest.patch.object(model, 'populate'):
            
            model._store({"name": "test"}, is_updating=False, is_saving=False)
            
            mock_before.assert_called_once()
            mock_after.assert_called_once()

    def test_complex_query_scenarios(self, mock_db_session):
        """Test complex query building scenarios."""
        # Setup mock attributes on MockUser
        MockUser.id = Mock()
        MockUser.name = Mock()
        MockUser.email = Mock()
        MockUser.age = Mock()
        
        # Mock the attribute resolution for various operators
        MockUser.name.__eq__ = Mock(return_value="name_eq")
        MockUser.name.ilike = Mock(return_value="name_ilike")
        MockUser.age.__gte__ = Mock(return_value="age_gte")
        MockUser.age.between = Mock(return_value="age_between")
        MockUser.email.in_ = Mock(return_value="email_in")
        
        qb = QueryBuilder(mock_db_session, MockUser)
        
        # Test complex filter combinations
        filters = {
            "name__ilike": "%john%",
            "age__gte": 18,
            "age__between": [18, 65],
            "email__in": ["john@example.com", "jane@example.com"]
        }
        
        predicates = qb._build_predicates(filters)
        assert len(predicates) == 4
        
        # Verify the mocked methods were called
        MockUser.name.ilike.assert_called_with("%john%")
        MockUser.age.__gte__.assert_called_with(18)
        MockUser.age.between.assert_called_with(18, 65)
        MockUser.email.in_.assert_called_with(["john@example.com", "jane@example.com"])

    def test_model_attribute_proxying(self):
        """Test attribute proxying between model and entity."""
        user_entity = MockUser()
        user_entity.custom_field = "test_value"
        
        model = UserModel(entity=user_entity)
        
        # Test getting attribute
        assert model.custom_field == "test_value"
        
        # Test setting attribute
        model.another_field = "another_value"
        assert user_entity.another_field == "another_value"
        
        # Test iteration
        user_entity.id = 1
        user_entity.name = "John"
        
        items = dict(model)
        # Should contain entity attributes except _sa_instance_state
        assert "id" in items
        assert "name" in items
        assert "custom_field" in items
        assert "_sa_instance_state" not in items
