if __name__ == "__main__":
    session = get_session()
    
    # Create a list of all teams
    teams = [
        real_madrid,
        bayern_munich,
        inter_milan,
        paris_saint_germain,
        barcelona,
        borussia_dortmund
    ]
    
    try:
        # Add all teams to the session
        session.add_all(teams)
        
        # Commit the changes
        session.commit()
        print("Successfully added all teams to the database!")
        
    except Exception as e:
        # Rollback in case of error
        session.rollback()
        print(f"An error occurred: {str(e)}")
        
    finally:
        # Always close the session
        session.close() 