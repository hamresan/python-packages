"""
Simple smoke tests for ham-orm basic functionality.
"""
import pytest
from ham_orm import AppBaseModel, QueryBuilder, dualmethod


def test_basic_imports():
    """Test that basic imports work."""
    assert AppBaseModel is not None
    assert QueryBuilder is not None  
    assert dualmethod is not None


def test_dualmethod_basic():
    """Test basic dualmethod functionality."""
    class TestClass:
        def __init__(self):
            self.value = "test"
            
        @dualmethod
        def get_value(self):
            return self.value
    
    # Test on instance
    instance = TestClass()
    assert instance.get_value() == "test"
    
    # Test on class (creates new instance)
    assert TestClass.get_value() == "test"


def test_query_builder_basic():
    """Test basic QueryBuilder initialization."""
    from unittest.mock import Mock
    
    session = Mock()
    session.query = Mock()  # Legacy support
    model = Mock()
    model.__name__ = "TestModel"
    
    qb = QueryBuilder(session, model)
    assert qb.db == session
    assert qb.model == model
    assert qb._use_legacy_query is True


def test_app_base_model_basic():
    """Test basic AppBaseModel functionality without complex mocking."""
    
    # Create a simple mock model
    class SimpleModel:
        __name__ = "SimpleModel"
        __tablename__ = "simple"
        
        def __init__(self):
            self.id = None
            self.name = None
    
    class TestModel(AppBaseModel):
        _model = SimpleModel
    
    # Test basic initialization
    model = TestModel()
    assert model._model == SimpleModel
    assert hasattr(model, '_entity')
    assert isinstance(model._entity, SimpleModel)


if __name__ == "__main__":
    pytest.main([__file__])
