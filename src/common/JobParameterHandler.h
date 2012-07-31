/*
 * JobParameterHandler.h
 *
 *  Created on: Mar 12, 2012
 *      Author: simonm
 */

#ifndef JOBPARAMETERHANDLER_H_
#define JOBPARAMETERHANDLER_H_

#include <map>
#include <string>
#include <vector>
#include <boost/lexical_cast.hpp>

using namespace std;
using namespace boost;

namespace fts3 { namespace common {

/**
 * The JobParameterHandler class contains list of string values corresponding
 * to transfer job parameter names. Moreover, it allows for mapping the
 * parameter names into the respective values.
 */
class JobParameterHandler {
public:

	/**
	 * Default constructor.
	 *
	 * Sets the default values for some job parameters,
	 * e.g. copy_pin_lifetime = 1
	 */
	JobParameterHandler();

	/**
	 * Destructor.
	 */
	virtual ~JobParameterHandler();

	/**
	 * The functional operator.
	 *
	 * Allows for assigning values to some chosen job parameters
	 *
	 * @param keys - vector with keys (e.g. keys[0] corresponds to values[0], and so on)
	 * @param values - vector with values (e.g. keys[0] corresponds to values[0], and so on)
	 */
	void operator() (vector<string>& keys, vector<string>& values);

	///@{
	/**
	 * names of transfer job parameters
	 */
	static const string FTS3_PARAM_GRIDFTP;
	static const string FTS3_PARAM_MYPROXY;
	static const string FTS3_PARAM_DELEGATIONID;
	static const string FTS3_PARAM_SPACETOKEN;
	static const string FTS3_PARAM_SPACETOKEN_SOURCE;
	static const string FTS3_PARAM_COPY_PIN_LIFETIME;
	static const string FTS3_PARAM_LAN_CONNECTION;
	static const string FTS3_PARAM_FAIL_NEARLINE;
	static const string FTS3_PARAM_OVERWRITEFLAG;
	static const string FTS3_PARAM_CHECKSUM_METHOD;
	static const string FTS3_PARAM_REUSE;
	///@}

	/**
	 * Gets the value corresponding to given parameter name
	 *
	 * @param name - parameter name
	 *
	 * @return parameter value
	 */
	inline string get(string name) {
		return parameters[name];
	}

	/**
	 * Gets the value corresponding to given parameter name
	 *
	 * @param name - parameter name
	 * @param T - typpe of the returned value
	 *
	 * @return parameter value
	 */
	template<typename T>
	inline T get(string name) {
		return lexical_cast<T>(parameters[name]);
	}

	/**
	 * Checks if the given parameter has been set
	 *
	 * @param name - parameter name
	 *
	 * @return true if the parameter value has been set
	 */
	inline bool isParamSet(string name) {
		return parameters.find(name) != parameters.end();
	}

private:

	/// maps parameter names into values
	map<string, string> parameters;

};

}
}

#endif /* JOBPARAMETERHANDLER_H_ */
