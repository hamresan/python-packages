#!/usr/bin/env python3
"""
Example usage of ham-orm package.

This example demonstrates how to use ham-orm with SQLAlchemy models.
Run this example to see the package in action.
"""

from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from ham_orm import AppBaseModel, QueryBuilder

# Define SQLAlchemy base and model
Base = declarative_base()


class User(Base):
    __tablename__ = 'users'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(50), nullable=False)
    email = Column(String(100), unique=True, nullable=False)
    age = Column(Integer)


# Define ham-orm wrapper
class UserModel(AppBaseModel):
    _model = User
    _primary_key = "id"
    _guard_fields = ["id"]  # Protect ID from mass assignment
    
    def before_insert(self, data):
        """Add validation before insert."""
        if not data.get("email"):
            raise ValueError("Email is required")
        return data


def main():
    """Demonstrate ham-orm functionality."""
    print("🚀 ham-orm Example")
    print("==================")
    
    # Setup in-memory SQLite database
    engine = create_engine('sqlite:///:memory:', echo=False)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    # Initialize UserModel with database session
    UserModel.init_db(session)
    
    print("✅ Database initialized and UserModel configured")
    
    # 1. Insert new user
    print("\n📝 Creating new user...")
    user_data = {"name": "John Doe", "email": "john@example.com", "age": 30}
    new_user = UserModel.insert(user_data)
    
    if new_user:
        print(f"   ✅ User created with ID: {new_user.id}")
        print(f"   Name: {new_user.name}, Email: {new_user.email}, Age: {new_user.age}")
    else:
        print("   ❌ Failed to create user")
        return
    
    # 2. Find user by ID
    print(f"\n🔍 Finding user by ID {new_user.id}...")
    found_user = UserModel.find(new_user.id)
    if found_user:
        print(f"   ✅ Found user: {found_user.name} ({found_user.email})")
    else:
        print("   ❌ User not found")
    
    # 3. Create another user
    print("\n📝 Creating another user...")
    user2_data = {"name": "Jane Smith", "email": "jane@example.com", "age": 25}
    user2 = UserModel.insert(user2_data)
    if user2:
        print(f"   ✅ User created with ID: {user2.id}")
    
    # 4. Get all users with filtering
    print("\n📋 Getting all users...")
    all_users = UserModel.all()
    print(f"   ✅ Total users: {len(all_users)}")
    for user in all_users:
        print(f"   - {user.name} ({user.email}), Age: {user.age}")
    
    # 5. Update user
    print(f"\n✏️ Updating user {new_user.id}...")
    updated_user = UserModel.update({"id": new_user.id, "age": 31})
    if updated_user:
        print(f"   ✅ Updated user age to: {updated_user.age}")
    
    # 6. Use QueryBuilder for complex queries  
    print("\n🔧 Using QueryBuilder for advanced queries...")
    qb = QueryBuilder(session, User)
    
    # Find users older than 25
    adult_users = qb.where({"age__gt": 25}).all()
    print(f"   ✅ Found {len(adult_users)} users older than 25:")
    for user in adult_users:
        print(f"   - {user.name}, Age: {user.age}")
    
    # 7. Check existence
    print("\n🔍 Checking if email exists...")
    exists = UserModel.exist("email", "john@example.com")
    print(f"   ✅ john@example.com exists: {exists}")
    
    exists = UserModel.exist("email", "nonexistent@example.com")
    print(f"   ✅ nonexistent@example.com exists: {exists}")
    
    print("\n🎉 Example completed successfully!")
    print("    The ham-orm package is working correctly.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
