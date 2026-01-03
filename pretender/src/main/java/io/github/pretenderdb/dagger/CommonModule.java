package io.github.pretenderdb.dagger;

import dagger.Module;
import dagger.Provides;
import java.time.Clock;
import javax.inject.Singleton;

/**
 * The type Common module.
 */
@Module
public class CommonModule {

  /**
   * Instantiates a new Common module.
   */
  public CommonModule() {
    // Default constructor
  }

  /**
   * Clock clock.
   *
   * @return the clock
   */
  @Provides
  @Singleton
  Clock clock() {
    return Clock.systemUTC();
  }

}
