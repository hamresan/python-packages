"""
Error handling and edge case tests for ham-orm.
"""
import pytest
from unittest.mock import Mock, patch
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from ham_orm import AppBaseModel, QueryBuilder


class MockModel:
    """Mock SQLAlchemy model for error testing."""
    __name__ = "MockModel"
    __tablename__ = "mock_table"
    
    def __init__(self):
        self.id = None
        self.name = None
        self.__dict__["_sa_instance_state"] = Mock()


class ErrorTestModel(AppBaseModel):
    """Test model for error scenarios."""
    _model = MockModel


class TestErrorHandling:
    """Test error handling and edge cases."""

    @pytest.fixture
    def mock_db_session(self):
        """Create mock database session."""
        session = Mock()
        session.add = Mock()
        session.commit = Mock()
        session.rollback = Mock()
        session.refresh = Mock()
        session.query = Mock()
        return session

    def test_store_integrity_error(self, mock_db_session):
        """Test _store method handles IntegrityError correctly."""
        ErrorTestModel.init_db(mock_db_session)
        model = ErrorTestModel()
        
        # Mock commit to raise IntegrityError
        mock_db_session.commit.side_effect = IntegrityError("statement", "params", "orig")
        
        with patch.object(model, 'before_insert', return_value={"name": "test"}), \
             patch.object(model, 'after_insert', return_value=True), \
             patch.object(model, 'populate'):
            
            result = model._store({"name": "test"}, is_updating=False, is_saving=False)
            
            assert result is None  # Should return None on IntegrityError
            mock_db_session.rollback.assert_called_once()

    def test_store_sqlalchemy_error(self, mock_db_session):
        """Test _store method handles SQLAlchemyError correctly."""
        ErrorTestModel.init_db(mock_db_session)
        model = ErrorTestModel()
        
        # Mock commit to raise SQLAlchemyError
        mock_db_session.commit.side_effect = SQLAlchemyError("database error")
        
        with patch.object(model, 'before_insert', return_value={"name": "test"}), \
             patch.object(model, 'after_insert', return_value=True), \
             patch.object(model, 'populate'):
            
            result = model._store({"name": "test"}, is_updating=False, is_saving=False)
            
            assert result is None  # Should return None on SQLAlchemyError
            mock_db_session.rollback.assert_called_once()

    def test_store_general_exception(self, mock_db_session):
        """Test _store method re-raises general exceptions."""
        ErrorTestModel.init_db(mock_db_session)
        model = ErrorTestModel()
        
        # Mock populate to raise a general exception
        with patch.object(model, 'before_insert', return_value={"name": "test"}), \
             patch.object(model, 'populate', side_effect=ValueError("validation error")):
            
            with pytest.raises(ValueError, match="validation error"):
                model._store({"name": "test"}, is_updating=False, is_saving=False)
            
            mock_db_session.rollback.assert_called_once()

    def test_query_builder_attr_resolution_error(self, mock_db_session):
        """Test QueryBuilder handles attribute resolution errors."""
        qb = QueryBuilder(mock_db_session, MockModel)
        
        with pytest.raises(ValueError, match="MockModel has no attribute 'nonexistent'"):
            qb._resolve_attr(MockModel, "nonexistent")

    def test_query_builder_invalid_filter_operator(self, mock_db_session):
        """Test QueryBuilder handles invalid filter operators."""
        MockModel.name = Mock()
        qb = QueryBuilder(mock_db_session, MockModel)
        
        with pytest.raises(ValueError, match="Unsupported operator '__invalid'"):
            qb._build_predicates({"name__invalid": "test"})

    def test_query_builder_invalid_in_operator_type(self, mock_db_session):
        """Test QueryBuilder validates 'in' operator argument types."""
        MockModel.name = Mock()
        qb = QueryBuilder(mock_db_session, MockModel)
        
        with pytest.raises(TypeError, match="'name__in' expects a list/tuple/set"):
            qb._build_predicates({"name__in": "not_a_list"})

    def test_query_builder_invalid_between_operator_type(self, mock_db_session):
        """Test QueryBuilder validates 'between' operator argument types."""
        MockModel.age = Mock()
        qb = QueryBuilder(mock_db_session, MockModel)
        
        # Test with wrong number of elements
        with pytest.raises(TypeError, match="'age__between' expects a 2-tuple/list"):
            qb._build_predicates({"age__between": [1, 2, 3]})
        
        # Test with non-list/tuple
        with pytest.raises(TypeError, match="'age__between' expects a 2-tuple/list"):
            qb._build_predicates({"age__between": "not_a_list"})

    def test_app_base_model_no_entity_attribute_error(self):
        """Test AppBaseModel handles missing entity attributes properly."""
        model = ErrorTestModel()
        
        with pytest.raises(AttributeError):
            _ = model.nonexistent_attribute

    def test_app_base_model_private_attribute_access(self):
        """Test AppBaseModel handles private attribute access."""
        model = ErrorTestModel()
        
        with pytest.raises(AttributeError, match="ErrorTestModel has no attribute '_private'"):
            _ = model._private

    def test_populate_with_none_entity(self):
        """Test populate method behavior with various edge cases."""
        model = ErrorTestModel()
        
        # Test with None values in data
        model._entity.name = "original"
        model.populate({"name": None})
        assert model._entity.name is None
        
        # Test with empty string that should be stripped
        model.populate({"name": "   "})
        assert model._entity.name == ""

    def test_model_hooks_with_exceptions(self, mock_db_session):
        """Test model behavior when hooks raise exceptions."""
        
        class FailingHooksModel(AppBaseModel):
            _model = MockModel
            
            def before_insert(self, data):
                raise ValueError("Hook failed")
        
        FailingHooksModel.init_db(mock_db_session)
        model = FailingHooksModel()
        
        with pytest.raises(ValueError, match="Hook failed"):
            model._store({"name": "test"}, is_updating=False, is_saving=False)
        
        # Should still rollback on exception
        mock_db_session.rollback.assert_called_once()

    def test_update_without_existing_record(self, mock_db_session):
        """Test update operation when record doesn't exist."""
        ErrorTestModel.init_db(mock_db_session)
        model = ErrorTestModel()
        
        with patch.object(ErrorTestModel, 'find', return_value=None):
            with pytest.raises(LookupError, match="ErrorTestModel with id=999 not found"):
                model._store({"id": 999, "name": "test"}, is_updating=True)

    def test_save_method_pk_determination(self, mock_db_session):
        """Test save method correctly determines if it's updating or inserting."""
        ErrorTestModel.init_db(mock_db_session)
        
        # Test with entity that has ID (should update)
        model = ErrorTestModel()
        model._entity.id = 1
        
        with patch.object(model, '_store') as mock_store:
            model.save({"name": "test"})
            mock_store.assert_called_once_with({"name": "test"}, is_updating=True, is_saving=True)
        
        # Test with entity that has no ID but data contains ID (should update)
        model = ErrorTestModel()
        model._entity.id = None
        
        with patch.object(model, '_store') as mock_store:
            model.save({"id": 1, "name": "test"})
            mock_store.assert_called_once_with({"id": 1, "name": "test"}, is_updating=True, is_saving=True)
        
        # Test with no ID anywhere (should insert)
        model = ErrorTestModel()
        model._entity.id = None
        
        with patch.object(model, '_store') as mock_store:
            model.save({"name": "test"})
            mock_store.assert_called_once_with({"name": "test"}, is_updating=False, is_saving=True)

    def test_attribute_deletion_edge_cases(self):
        """Test edge cases in attribute deletion."""
        model = ErrorTestModel()
        
        # Test deleting non-existent attribute
        with pytest.raises(AttributeError):
            del model.nonexistent_attr
        
        # Test deleting private attribute (should use object.__delattr__)
        model._private_attr = "test"
        del model._private_attr
        assert not hasattr(model, "_private_attr")

    def test_string_representation_with_complex_data(self):
        """Test string representation with various data types."""
        model = ErrorTestModel()
        
        # Add various types of data to entity
        model._entity.id = 1
        model._entity.name = "test"
        model._entity.data = {"key": "value"}
        model._entity.numbers = [1, 2, 3]
        
        str_repr = str(model)
        
        # Should contain all fields except _sa_instance_state
        assert "id" in str_repr
        assert "name" in str_repr
        assert "data" in str_repr
        assert "numbers" in str_repr
        assert "_sa_instance_state" not in str_repr

    def test_iteration_with_edge_cases(self):
        """Test iteration behavior with edge cases."""
        model = ErrorTestModel()
        
        # Test with minimal entity
        model._entity.id = None
        model._entity.name = None
        
        items = list(model)
        filtered_items = [(k, v) for k, v in items if k != "_sa_instance_state"]
        
        assert ("id", None) in filtered_items
        assert ("name", None) in filtered_items
