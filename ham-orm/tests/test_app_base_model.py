"""
Tests for AppBaseModel functionality.
"""
import pytest
from unittest.mock import Mock, MagicMock, patch
from ham_orm import AppBaseModel, dualmethod


class MockSQLAlchemyModel:
    """Mock SQLAlchemy model for testing."""
    __name__ = "MockModel"
    __tablename__ = "mock_table"
    
    def __init__(self):
        self.id = None
        self.name = None
        self.__dict__["_sa_instance_state"] = Mock()


class TestModel(AppBaseModel):
    """Test model for testing AppBaseModel functionality."""
    _model = MockSQLAlchemyModel
    _primary_key = "id"


class TestDualMethod:
    """Test dualmethod decorator functionality."""

    def test_dualmethod_on_instance(self):
        """Test dualmethod called on instance."""
        class TestClass:
            def __init__(self):
                self.value = "instance"
                
            @dualmethod
            def get_value(self):
                return self.value
        
        instance = TestClass()
        result = instance.get_value()
        assert result == "instance"

    def test_dualmethod_on_class(self):
        """Test dualmethod called on class (creates new instance)."""
        class TestClass:
            def __init__(self):
                self.value = "class_instance"
                
            @dualmethod
            def get_value(self):
                return self.value
        
        result = TestClass.get_value()
        assert result == "class_instance"


class TestAppBaseModel:
    """Test AppBaseModel class functionality."""

    @pytest.fixture
    def mock_db_session(self):
        """Create a mock database session."""
        session = Mock()
        session.add = Mock()
        session.commit = Mock()
        session.rollback = Mock()
        session.refresh = Mock()
        return session

    @pytest.fixture
    def test_model_with_db(self, mock_db_session):
        """Create a test model with initialized database."""
        TestModel.init_db(mock_db_session)
        return TestModel

    def test_init_db_class_method(self, mock_db_session):
        """Test init_db class method."""
        result = TestModel.init_db(mock_db_session)
        assert TestModel._db == mock_db_session
        assert result == TestModel

    def test_model_initialization_without_model(self):
        """Test model initialization without _model set."""
        class InvalidModel(AppBaseModel):
            _model = None
        
        with pytest.raises(RuntimeError, match="Model is not initialized"):
            InvalidModel()

    def test_model_initialization_with_entity(self):
        """Test model initialization with existing entity."""
        entity = MockSQLAlchemyModel()
        model = TestModel(entity=entity)
        assert model._entity == entity

    def test_model_initialization_with_attrs(self):
        """Test model initialization with attributes."""
        with patch.object(TestModel, 'populate') as mock_populate:
            attrs = {"name": "test"}
            TestModel(attrs=attrs)
            mock_populate.assert_called_once_with(attrs)

    def test_guard_fields_property(self):
        """Test guard_fields property."""
        model = TestModel()
        guard_fields = model.guard_fields
        expected_fields = ["created_at", "creator", "updated_at", "updator"]
        for field in expected_fields:
            assert field in guard_fields

    def test_modelname_property(self):
        """Test modelname property."""
        model = TestModel()
        assert model.modelname == "MockModel"

    def test_tablename_property(self):
        """Test tablename property."""
        model = TestModel()
        assert model.tablename == "mock_table"

    def test_ensure_ready_without_db(self):
        """Test _ensure_ready raises error when db not initialized."""
        TestModel._db = None
        with pytest.raises(RuntimeError, match="Database session is not initialized"):
            TestModel._ensure_ready()

    def test_ensure_ready_without_model(self):
        """Test _ensure_ready raises error when model not set."""
        TestModel._db = Mock()
        original_model = TestModel._model
        TestModel._model = None
        
        with pytest.raises(RuntimeError, match="Model is not initialized"):
            TestModel._ensure_ready()
        
        # Restore original model
        TestModel._model = original_model

    def test_find_with_none_pk(self, test_model_with_db):
        """Test find method with None primary key."""
        result = test_model_with_db.find(None)
        assert result is None

    def test_exist_with_none_value(self, test_model_with_db):
        """Test exist method with None value."""
        result = test_model_with_db.exist("name", None)
        assert result is False

    def test_exist_with_valid_value(self, test_model_with_db):
        """Test exist method with valid value."""
        with patch.object(test_model_with_db, 'first', return_value=Mock()) as mock_first:
            result = test_model_with_db.exist("name", "test")
            assert result is True
            mock_first.assert_called_once()

    def test_exist_with_exclude_value(self, test_model_with_db):
        """Test exist method with exclude_value."""
        with patch.object(test_model_with_db, 'first', return_value=None) as mock_first:
            result = test_model_with_db.exist("name", "test", exclude_value=1)
            assert result is False
            # Check that the filters include the exclude condition
            call_args = mock_first.call_args
            filters = call_args[1]['filters']
            assert "id__ne" in filters
            assert filters["id__ne"] == 1

    def test_populate_method(self):
        """Test populate method."""
        model = TestModel()
        model._entity.name = None
        
        data = {"name": "  test  "}  # With whitespace
        model.populate(data)
        
        assert model._entity.name == "test"  # Should be stripped

    def test_populate_with_guard_fields(self):
        """Test populate method with guard fields."""
        model = TestModel()
        model._guard_fields = ["protected_field"]
        model._entity.protected_field = "original"
        
        data = {"protected_field": "new_value"}
        model.populate(data)
        
        # Should not be updated because it's in guard fields
        assert model._entity.protected_field == "original"

    def test_populate_with_whitelist_fields(self):
        """Test populate method with whitelist fields."""
        class WhitelistModel(AppBaseModel):
            _model = MockSQLAlchemyModel
            _whitelist_fields = ["name"]
        
        model = WhitelistModel()
        model._entity.name = None
        model._entity.other_field = "original"
        
        data = {"name": "allowed", "other_field": "not_allowed"}
        model.populate(data)
        
        assert model._entity.name == "allowed"
        assert model._entity.other_field == "original"  # Should not change

    def test_insert_method(self, test_model_with_db):
        """Test insert method."""
        with patch.object(test_model_with_db, '_store') as mock_store:
            data = {"name": "test"}
            test_model_with_db.insert(data)
            mock_store.assert_called_once_with(data, is_updating=False, is_saving=False)

    def test_update_method(self, test_model_with_db):
        """Test update method."""
        with patch.object(test_model_with_db, '_store') as mock_store:
            data = {"name": "test"}
            test_model_with_db.update(data)
            mock_store.assert_called_once_with(data, is_updating=True, is_saving=False)

    def test_save_method_for_new_entity(self, test_model_with_db):
        """Test save method for new entity (no ID)."""
        model = test_model_with_db()
        model._entity.id = None
        
        with patch.object(model, '_store') as mock_store:
            data = {"name": "test"}
            model.save(data)
            mock_store.assert_called_once_with(data, is_updating=False, is_saving=True)

    def test_save_method_for_existing_entity(self, test_model_with_db):
        """Test save method for existing entity (has ID)."""
        model = test_model_with_db()
        model._entity.id = 1
        
        with patch.object(model, '_store') as mock_store:
            data = {"name": "test"}
            model.save(data)
            mock_store.assert_called_once_with(data, is_updating=True, is_saving=True)

    def test_store_insert_success(self, test_model_with_db, mock_db_session):
        """Test _store method for successful insert."""
        model = test_model_with_db()
        
        with patch.object(model, 'before_insert', return_value={"name": "test"}) as mock_before, \
             patch.object(model, 'after_insert', return_value=True) as mock_after, \
             patch.object(model, 'populate') as mock_populate:
            
            result = model._store({"name": "test"}, is_updating=False, is_saving=False)
            
            assert result == model
            mock_before.assert_called_once_with({"name": "test"})
            mock_after.assert_called_once()
            mock_populate.assert_called_once_with({"name": "test"})
            mock_db_session.add.assert_called_once_with(model._entity)
            mock_db_session.commit.assert_called_once()
            mock_db_session.refresh.assert_called_once_with(model._entity)

    def test_store_insert_after_hook_fails(self, test_model_with_db, mock_db_session):
        """Test _store method when after_insert returns False."""
        model = test_model_with_db()
        
        with patch.object(model, 'before_insert', return_value={"name": "test"}), \
             patch.object(model, 'after_insert', return_value=False), \
             patch.object(model, 'populate'):
            
            result = model._store({"name": "test"}, is_updating=False, is_saving=False)
            
            assert result is None
            mock_db_session.rollback.assert_called_once()

    def test_store_update_success(self, test_model_with_db, mock_db_session):
        """Test _store method for successful update."""
        model = test_model_with_db()
        model._entity.id = 1
        
        with patch.object(test_model_with_db, 'find', return_value=model), \
             patch.object(model, 'before_update', return_value={"name": "updated"}) as mock_before, \
             patch.object(model, 'after_update', return_value=True) as mock_after, \
             patch.object(model, 'populate') as mock_populate:
            
            result = model._store({"name": "updated"}, is_updating=True, is_saving=False)
            
            assert result == model
            mock_before.assert_called_once_with({"name": "updated"})
            mock_after.assert_called_once()
            mock_populate.assert_called_once_with({"name": "updated"})
            mock_db_session.commit.assert_called_once()

    def test_store_update_not_found(self, test_model_with_db):
        """Test _store method when entity not found for update."""
        model = test_model_with_db()
        
        with patch.object(test_model_with_db, 'find', return_value=None):
            with pytest.raises(LookupError, match="TestModel with id=1 not found"):
                model._store({"id": 1, "name": "updated"}, is_updating=True)

    def test_store_update_no_pk(self, test_model_with_db):
        """Test _store method for update without primary key."""
        model = test_model_with_db()
        model._entity.id = None
        
        with pytest.raises(ValueError, match="TestModel update requires 'id'"):
            model._store({"name": "updated"}, is_updating=True)

    def test_attribute_proxying_getattr(self):
        """Test __getattr__ proxying to entity."""
        model = TestModel()
        model._entity.custom_attr = "test_value"
        
        assert model.custom_attr == "test_value"

    def test_attribute_proxying_getattr_nonexistent(self):
        """Test __getattr__ with nonexistent attribute."""
        model = TestModel()
        
        with pytest.raises(AttributeError):
            _ = model.nonexistent_attr

    def test_attribute_proxying_getattr_private(self):
        """Test __getattr__ with private attribute."""
        model = TestModel()
        
        with pytest.raises(AttributeError):
            _ = model._private_attr

    def test_attribute_proxying_setattr(self):
        """Test __setattr__ proxying to entity."""
        model = TestModel()
        model.custom_attr = "test_value"
        
        assert model._entity.custom_attr == "test_value"

    def test_attribute_proxying_delattr(self):
        """Test __delattr__ proxying to entity."""
        model = TestModel()
        model._entity.custom_attr = "test_value"
        
        del model.custom_attr
        assert not hasattr(model._entity, "custom_attr")

    def test_iteration(self):
        """Test __iter__ method."""
        model = TestModel()
        model._entity.id = 1
        model._entity.name = "test"
        
        items = list(model)
        # Filter out _sa_instance_state
        items = [(k, v) for k, v in items if k != "_sa_instance_state"]
        
        assert len(items) >= 2
        assert ("id", 1) in items
        assert ("name", "test") in items

    def test_string_representation(self):
        """Test __str__ method."""
        model = TestModel()
        model._entity.id = 1
        model._entity.name = "test"
        
        str_repr = str(model)
        assert "id" in str_repr
        assert "name" in str_repr
        assert "_sa_instance_state" not in str_repr

    def test_before_hooks_default_behavior(self):
        """Test that before_* hooks return data unchanged by default."""
        model = TestModel()
        
        data = {"name": "test"}
        assert model.before_save(data) == data
        assert model.before_insert(data) == data
        assert model.before_update(data) == data

    def test_after_hooks_default_behavior(self):
        """Test that after_* hooks return True by default."""
        model = TestModel()
        
        assert model.after_save(None) is True
        assert model.after_insert() is True
        assert model.after_update(None) is True
