"""
Tests for QueryBuilder functionality.
"""
import pytest
from unittest.mock import Mock, MagicMock
from ham_orm import QueryBuilder


class TestQueryBuilder:
    """Test QueryBuilder class functionality."""

    @pytest.fixture
    def mock_db_session(self):
        """Create a mock database session."""
        session = Mock()
        session.query = Mock()
        return session

    @pytest.fixture
    def mock_model(self):
        """Create a mock SQLAlchemy model."""
        model = Mock()
        model.__name__ = "TestModel"
        model.id = Mock()
        model.name = Mock()
        return model

    def test_query_builder_initialization_with_legacy_query(self, mock_db_session, mock_model):
        """Test QueryBuilder initialization with legacy query support."""
        qb = QueryBuilder(mock_db_session, mock_model)
        assert qb.db == mock_db_session
        assert qb.model == mock_model
        assert qb._use_legacy_query is True

    def test_query_builder_initialization_without_legacy_query(self, mock_model):
        """Test QueryBuilder initialization without legacy query support."""
        modern_session = Mock()
        # Remove query attribute to simulate modern SQLAlchemy
        delattr(modern_session, 'query') if hasattr(modern_session, 'query') else None
        
        qb = QueryBuilder(modern_session, mock_model)
        assert qb.db == modern_session
        assert qb.model == mock_model
        assert qb._use_legacy_query is False

    def test_include_with_string(self, mock_db_session, mock_model):
        """Test include method with string relationship name."""
        qb = QueryBuilder(mock_db_session, mock_model)
        mock_model.relationship = Mock()
        
        result = qb.include("relationship")
        assert result is qb  # Should return self for chaining
        assert len(qb._includes) == 1

    def test_only_with_string(self, mock_db_session, mock_model):
        """Test only method with string column name."""
        qb = QueryBuilder(mock_db_session, mock_model)
        mock_model.name = Mock()
        
        result = qb.only("name")
        assert result is qb  # Should return self for chaining
        assert len(qb._only_cols) == 1

    def test_where_with_filters_dict(self, mock_db_session, mock_model):
        """Test where method with filters dictionary."""
        qb = QueryBuilder(mock_db_session, mock_model)
        mock_model.name = Mock()
        mock_model.name.__eq__ = Mock(return_value="name_equals_filter")
        
        result = qb.where({"name": "test"})
        assert result is qb  # Should return self for chaining

    def test_order_by_with_string(self, mock_db_session, mock_model):
        """Test order_by method with string field name."""
        qb = QueryBuilder(mock_db_session, mock_model)
        mock_model.name = Mock()
        
        result = qb.order_by("name")
        assert result is qb  # Should return self for chaining
        assert len(qb._order_by) == 1

    def test_order_by_with_desc_string(self, mock_db_session, mock_model):
        """Test order_by method with descending string field name."""
        qb = QueryBuilder(mock_db_session, mock_model)
        mock_model.name = Mock()
        
        result = qb.order_by("-name")
        assert result is qb  # Should return self for chaining
        assert len(qb._order_by) == 1

    def test_limit_and_offset(self, mock_db_session, mock_model):
        """Test limit and offset methods."""
        qb = QueryBuilder(mock_db_session, mock_model)
        
        result_limit = qb.limit(10)
        assert result_limit is qb
        assert qb._limit == 10
        
        result_offset = qb.offset(5)
        assert result_offset is qb
        assert qb._offset == 5

    def test_build_query_method(self, mock_db_session, mock_model):
        """Test build_query convenience method."""
        qb = QueryBuilder(mock_db_session, mock_model)
        mock_model.name = Mock()
        mock_model.id = Mock()
        
        result = qb.build_query(
            fields=["name"],
            filters={"id": 1},
            orders=["name"],
            includes=["relationship"],
            offset=0,
            limit=10
        )
        
        assert result is qb

    def test_resolve_attr_existing(self, mock_db_session, mock_model):
        """Test _resolve_attr with existing attribute."""
        qb = QueryBuilder(mock_db_session, mock_model)
        mock_model.name = Mock()
        
        result = qb._resolve_attr(mock_model, "name")
        assert result == mock_model.name

    def test_resolve_attr_nonexistent(self, mock_db_session, mock_model):
        """Test _resolve_attr with nonexistent attribute."""
        qb = QueryBuilder(mock_db_session, mock_model)
        
        with pytest.raises(ValueError, match="TestModel has no attribute 'nonexistent'"):
            qb._resolve_attr(mock_model, "nonexistent")

    def test_normalize_field(self, mock_db_session, mock_model):
        """Test _normalize_field method."""
        qb = QueryBuilder(mock_db_session, mock_model)
        
        # Test with prefixed field
        result = qb._normalize_field("TestModel.name")
        assert result == "name"
        
        # Test with non-prefixed field
        result = qb._normalize_field("name")
        assert result == "name"

    def test_build_predicates_eq_operator(self, mock_db_session, mock_model):
        """Test _build_predicates with eq operator."""
        qb = QueryBuilder(mock_db_session, mock_model)
        mock_model.name = Mock()
        mock_model.name.__eq__ = Mock(return_value="name_eq_predicate")
        
        predicates = qb._build_predicates({"name": "test"})
        assert len(predicates) == 1
        mock_model.name.__eq__.assert_called_once_with("test")

    def test_build_predicates_ne_operator(self, mock_db_session, mock_model):
        """Test _build_predicates with ne operator."""
        qb = QueryBuilder(mock_db_session, mock_model)
        mock_model.name = Mock()
        mock_model.name.__ne__ = Mock(return_value="name_ne_predicate")
        
        predicates = qb._build_predicates({"name__ne": "test"})
        assert len(predicates) == 1
        mock_model.name.__ne__.assert_called_once_with("test")

    def test_build_predicates_in_operator(self, mock_db_session, mock_model):
        """Test _build_predicates with in operator."""
        qb = QueryBuilder(mock_db_session, mock_model)
        mock_model.name = Mock()
        mock_model.name.in_ = Mock(return_value="name_in_predicate")
        
        predicates = qb._build_predicates({"name__in": ["test1", "test2"]})
        assert len(predicates) == 1
        mock_model.name.in_.assert_called_once_with(["test1", "test2"])

    def test_build_predicates_between_operator(self, mock_db_session, mock_model):
        """Test _build_predicates with between operator."""
        qb = QueryBuilder(mock_db_session, mock_model)
        mock_model.age = Mock()
        mock_model.age.between = Mock(return_value="age_between_predicate")
        
        predicates = qb._build_predicates({"age__between": [18, 65]})
        assert len(predicates) == 1
        mock_model.age.between.assert_called_once_with(18, 65)

    def test_build_predicates_like_operator(self, mock_db_session, mock_model):
        """Test _build_predicates with like operator."""
        qb = QueryBuilder(mock_db_session, mock_model)
        mock_model.name = Mock()
        mock_model.name.like = Mock(return_value="name_like_predicate")
        
        predicates = qb._build_predicates({"name__like": "%test%"})
        assert len(predicates) == 1
        mock_model.name.like.assert_called_once_with("%test%")

    def test_build_predicates_ilike_operator(self, mock_db_session, mock_model):
        """Test _build_predicates with ilike operator."""
        qb = QueryBuilder(mock_db_session, mock_model)
        mock_model.name = Mock()
        mock_model.name.ilike = Mock(return_value="name_ilike_predicate")
        
        predicates = qb._build_predicates({"name__ilike": "%test%"})
        assert len(predicates) == 1
        mock_model.name.ilike.assert_called_once_with("%test%")

    def test_build_predicates_invalid_operator(self, mock_db_session, mock_model):
        """Test _build_predicates with invalid operator."""
        qb = QueryBuilder(mock_db_session, mock_model)
        mock_model.name = Mock()
        
        with pytest.raises(ValueError, match="Unsupported operator '__invalid'"):
            qb._build_predicates({"name__invalid": "test"})

    def test_build_predicates_in_operator_invalid_type(self, mock_db_session, mock_model):
        """Test _build_predicates with in operator and invalid value type."""
        qb = QueryBuilder(mock_db_session, mock_model)
        mock_model.name = Mock()
        
        with pytest.raises(TypeError, match="'name__in' expects a list/tuple/set"):
            qb._build_predicates({"name__in": "not_a_list"})

    def test_build_predicates_between_operator_invalid_type(self, mock_db_session, mock_model):
        """Test _build_predicates with between operator and invalid value type."""
        qb = QueryBuilder(mock_db_session, mock_model)
        mock_model.age = Mock()
        
        with pytest.raises(TypeError, match="'age__between' expects a 2-tuple/list"):
            qb._build_predicates({"age__between": [1, 2, 3]})  # Too many values

    def test_exists_method(self, mock_db_session, mock_model):
        """Test exists method."""
        qb = QueryBuilder(mock_db_session, mock_model)
        qb.first = Mock(return_value="some_result")
        
        result = qb.exists()
        assert result is True
        
        qb.first = Mock(return_value=None)
        result = qb.exists()
        assert result is False
